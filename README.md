# ChronosDB

## Introduction/Overview

ChronosDB is a basic, append-only time-series database (TSDB) written in Go. It serves primarily as a learning project to explore and implement the core mechanics of a TSDB from scratch. The focus is on understanding fundamental concepts like data ingestion, storage, querying, and aggregation in a time-series context.

## Features

*   **Metric-based Data Storage:** Data is organized and stored per metric.
*   **Timestamped Values:** Supports storing `float64` values associated with a `time.Time` timestamp.
*   **Append-only File-based Persistence:** Data is written to files in an append-only fashion.
*   **Range Queries:** Allows querying for raw data points within a specified time range for a given metric.
*   **Aggregation Functions:** Supports time-based aggregation of data using functions: `sum`, `count`, `min`, `max`, `avg`.
*   **Basic On-Demand Downsampling:** Provides functionality to pre-aggregate data from a source metric into a new, downsampled metric.

## Design Choices & Storage Format

### Overall Philosophy

ChronosDB adopts a simple, file-based, append-only storage model.

### Directory Structure

*   **Base Directory:** Specified at DB initialization (e.g., `./mytsdb_data/`). The examples will use this relative path.
*   **Metric-Specific Directories:** Within the base directory, each metric has its own subdirectory (e.g., `./mytsdb_data/cpu.load_avg.1m/`).

### Segment Files

Data for each metric is divided into segment files.

*   **Filename Format:** Segment files are named using the Unix nanosecond timestamp of the *first data point* that initiates the segment file (e.g., `1678886400000000000.dat`).

### Block Structure (within Segment Files)

Segment files consist of blocks. `pointsPerBlock` is `1024`.

*   **Binary Format of a Block (Columnar):**
    1.  **Header (2 bytes):** `uint16` (BigEndian) - number of data points (`numPoints`) in the block.
    2.  **Timestamps (`numPoints * 8` bytes):** `numPoints` `int64` Unix nanosecond timestamps (BigEndian).
    3.  **Values (`numPoints * 8` bytes):** `numPoints` `float64` values (encoded as `uint64` via `math.Float64bits`, BigEndian).

## Core API Usage (Go Library)

(Module: `chronosdb`, package path `chronosdb/internal/chronosdb`)

### Initialization and Full Example

```go
package main

import (
	"log"
	"os"
	"time"
	"fmt" 

	"chronosdb/internal/chronosdb" 
)

func main() {
	dataDir := "./mytsdb_data"
	// Optional: os.RemoveAll(dataDir) for a fresh run
	// if err := os.RemoveAll(dataDir); err != nil {
	//    log.Fatalf("Failed to remove old data: %v", err)
	// }

	db, err := chronosdb.NewDB(dataDir) 
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	defer func() {
		fmt.Println("Closing DB in main defer...")
		if errClose := db.Close(); errClose != nil {
			log.Printf("Failed to close DB: %v", errClose)
		}
	}()

	log.Println("ChronosDB initialized.")
	metricName := "cpu.temp"
	baseTime := time.Now().Add(-2 * time.Hour).Truncate(time.Second)

	// Writing Data
	log.Println("Writing data...")
	for i := 0; i < 120; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Minute)
		value := 60.0 + (float64(i%10) * 1.5)
		if errWrite := db.Write(metricName, ts, value); errWrite != nil {
			log.Printf("Write error: %v", errWrite)
		}
	}
	log.Println("Finished writing data.")
	
	log.Println("Closing DB to flush and reopen for query demonstration...")
    if errClose := db.Close(); errClose != nil { 
        log.Fatalf("Failed to close DB before querying: %v", errClose)
    }
    // db is re-assigned here. The main defer will apply to this new instance.
    db, err = chronosdb.NewDB(dataDir) 
    if err != nil {
        log.Fatalf("Failed to reopen DB for querying: %v", err)
    }

	// Querying Data
	log.Println("\nQuerying data...")
	queryStartTime := baseTime.Add(30 * time.Minute)
	queryEndTime := baseTime.Add(40 * time.Minute)
	points, errQuery := db.Query(metricName, queryStartTime, queryEndTime)
	if errQuery != nil {
		log.Printf("Query error: %v", errQuery)
	} else {
		log.Printf("Found %d points for '%s':", len(points), metricName)
		for _, p := range points {
			log.Printf("  Time=%s, Value=%.2f", p.Timestamp.Format(time.RFC3339), p.Value)
		}
	}

	// Aggregating Data
	log.Println("\nAggregating data...")
	aggResults, errAgg := db.Aggregate(metricName, baseTime, baseTime.Add(1*time.Hour), 10*time.Minute, chronosdb.AggAvg)
	if errAgg != nil {
		log.Printf("Aggregation error: %v", errAgg)
	} else {
		log.Printf("Aggregated data for '%s' (Avg over 10m):", metricName)
		for _, ar := range aggResults {
			log.Printf("  IntervalStart=%s, AvgValue=%.2f", ar.Timestamp.Format(time.RFC3339), ar.AggregatedValue)
		}
	}

	// Downsampling Data
	log.Println("\nDownsampling data...")
	dsParams := chronosdb.DownsampleParams{
		OriginalMetricName:    metricName,
		DownsampledMetricName: metricName + ".avg_30m", 
		QueryStartTime:        baseTime,
		QueryEndTime:          baseTime.Add(2 * time.Hour), 
		Interval:              30 * time.Minute,
		AggFunc:               chronosdb.AggAvg,
	}
	if errDs := db.Downsample(dsParams); errDs != nil {
		log.Printf("Downsampling error: %v", errDs)
	} else {
		log.Printf("Downsampling successful for '%s'. Querying downsampled data:", dsParams.DownsampledMetricName)
		dsPoints, dsQErr := db.Query(dsParams.DownsampledMetricName, dsParams.QueryStartTime, dsParams.QueryEndTime)
		if dsQErr != nil {
			log.Printf("Error querying downsampled metric: %v", dsQErr)
		} else {
			for _, p := range dsPoints {
				log.Printf("  Downsampled Point: Time=%s, Value=%.2f", p.Timestamp.Format(time.RFC3339), p.Value)
			}
		}
	}
}
```

### Writing Data (Simplified Snippet)

```go
// Assume 'db' is an initialized *chronosdb.DB instance
metric := "cpu.temp"
ts := time.Now()
value := 75.5
if err := db.Write(metric, ts, value); err != nil {
    log.Printf("Failed to write data: %v", err)
}
```

### Querying Data (Simplified Snippet)

```go
// Assume 'db' is an initialized *chronosdb.DB instance
startTime := time.Now().Add(-1 * time.Hour)
endTime := time.Now()
points, err := db.Query("cpu.temp", startTime, endTime)
if err != nil {
    log.Printf("Query failed: %v", err)
}
for _, p := range points {
    // p.MetricName is implicitly "cpu.temp" due to the query
    log.Printf("DataPoint: Time=%s, Value=%.2f", p.Timestamp.Format(time.RFC3339), p.Value)
}
```

### Aggregating Data (Simplified Snippet)

```go
// Assume 'db' is an initialized *chronosdb.DB instance
startTime := time.Now().Add(-1 * time.Hour)
endTime := time.Now()
interval := 10 * time.Minute
aggResults, err := db.Aggregate("cpu.temp", startTime, endTime, interval, chronosdb.AggAvg)
if err != nil {
    log.Printf("Aggregation failed: %v", err)
}
for _, ar := range aggResults {
    log.Printf("Aggregated: Time=%s, AvgValue=%.2f", ar.Timestamp.Format(time.RFC3339), ar.AggregatedValue)
}
```

### Downsampling Data (Simplified Snippet)

```go
// Assume 'db' is an initialized *chronosdb.DB instance
startTime := time.Now().Add(-24 * time.Hour) 
endTime := time.Now()
params := chronosdb.DownsampleParams{
    OriginalMetricName:    "cpu.temp.raw",        
    DownsampledMetricName: "cpu.temp.avg_10m",    
    QueryStartTime:        startTime,
    QueryEndTime:          endTime,
    Interval:              10 * time.Minute,      
    AggFunc:               chronosdb.AggAvg,      
}
if err := db.Downsample(params); err != nil {
    log.Printf("Downsampling failed: %v", err)
} else {
    log.Println("Downsampling completed.")
}
```

## How to Build/Run (Locally)

*   **As a Library:** This project is primarily a Go library.
*   **Running Tests:** `go test ./internal/chronosdb/...` (from the project root).
*   **Using in Your App:** Import `chronosdb/internal/chronosdb` (assuming `chronosdb` is your Go module name).

## Future Considerations/Potential Improvements

*   Indexing for faster queries.
*   Advanced segment management (time/size-based rotation).
*   Data compression.
*   Enhanced concurrency control.
*   HTTP API or CLI.
*   Smarter query engine (e.g., auto-use of downsampled data).
*   Write-Ahead Logging (WAL).
*   Configuration for parameters like `pointsPerBlock`.
*   Data retention policies.
```
