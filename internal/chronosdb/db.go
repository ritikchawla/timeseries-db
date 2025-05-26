package chronosdb

import (
	"fmt"
	"os"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"math"
	"time"
)

// AggregationType defines the type of aggregation to perform.
type AggregationType string

// Defines the supported aggregation functions.
const (
	AggSum   AggregationType = "sum"
	AggCount AggregationType = "count"
	AggMin   AggregationType = "min"
	AggMax   AggregationType = "max"
	AggAvg   AggregationType = "avg"
)

// DataPoint is defined in core.go, but needed for the Query method signature
// and return type. Assuming core.go is in the same package chronosdb.
// (No actual change needed here, just a comment for clarity)

// DB manages the time-series database.
type DB struct {
	basePath     string
	mu           sync.RWMutex
	openSegments map[string]*SegmentManager // Key: metricName
}

// SegmentManager manages writes to a single metric's active segment file.
type SegmentManager struct {
	metricName           string
	dbBasePath           string // To construct full paths
	filePath             string // Path to the current segment file
	file                 *os.File
	currentBlockTimestamps []int64   // Buffer for timestamps of the current block
	currentBlockValues   []float64 // Buffer for values of the current block
	pointsInCurrentBlock int       // Counter for points in the current block
	mu                   sync.Mutex // To protect this specific segment manager's state
}

// NewDB creates a new DB instance.
// It creates the basePath directory if it doesn't exist.
func NewDB(basePath string) (*DB, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base data directory %s: %w", basePath, err)
	}

	return &DB{
		basePath:     basePath,
		openSegments: make(map[string]*SegmentManager),
	}, nil
}

// Close flushes all pending data to disk and closes open segment files.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var firstErr error
	for metricName, sm := range db.openSegments {
		fmt.Printf("Closing segment for metric %s\n", metricName) // Logging
		if err := sm.Close(); err != nil {
			fmt.Printf("Error closing segment for metric %s: %v\n", metricName, err) // Logging
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close segment for metric %s: %w", metricName, err)
			}
		}
		delete(db.openSegments, metricName)
	}
	return firstErr
}

// Close flushes any pending block data and closes the segment file.
func (sm *SegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.pointsInCurrentBlock > 0 {
		fmt.Printf("Flushing remaining %d points for metric %s before closing file %s\n", sm.pointsInCurrentBlock, sm.metricName, sm.filePath) // Logging
		if err := sm.flushBlock(); err != nil { // Use internal flushBlock which doesn't reset points
			return fmt.Errorf("failed to flush block on close for metric %s: %w", sm.metricName, err)
		}
		// Reset after successful flush
		sm.currentBlockTimestamps = make([]int64, 0, pointsPerBlock)
		sm.currentBlockValues = make([]float64, 0, pointsPerBlock)
		sm.pointsInCurrentBlock = 0
	}

	if sm.file != nil {
		fmt.Printf("Closing file %s for metric %s\n", sm.filePath, sm.metricName) // Logging
		err := sm.file.Close()
		sm.file = nil
		sm.filePath = ""
		if err != nil {
			return fmt.Errorf("failed to close segment file for metric %s: %w", sm.metricName, err)
		}
	}
	return nil
}

// Write ingests a single data point into the database.
func (db *DB) Write(metricName string, timestamp time.Time, value float64) error {
	db.mu.Lock() // Lock for accessing/modifying openSegments map
	sm, ok := db.openSegments[metricName]
	if !ok {
		metricPath := filepath.Join(db.basePath, metricName)
		if err := os.MkdirAll(metricPath, 0755); err != nil {
			db.mu.Unlock()
			return fmt.Errorf("failed to create metric directory %s: %w", metricPath, err)
		}
		sm = &SegmentManager{
			metricName:           metricName,
			dbBasePath:           db.basePath,
			currentBlockTimestamps: make([]int64, 0, pointsPerBlock),
			currentBlockValues:   make([]float64, 0, pointsPerBlock),
		}
		db.openSegments[metricName] = sm
	}
	db.mu.Unlock() // Unlock map access before calling segment manager

	return sm.AddDataPoint(timestamp.UnixNano(), value)
}

// AddDataPoint adds a data point to the segment.
// It buffers points and flushes them as a block when pointsPerBlock is reached.
func (sm *SegmentManager) AddDataPoint(tsNano int64, val float64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// If this is the first point for a new segment file (sm.file == nil)
	// and also the first point in the current block, this timestamp will name the file.
	if sm.file == nil && sm.pointsInCurrentBlock == 0 {
		// This logic ensures the filename is based on the actual first point
		// that initiates this specific segment file.
		segmentFileName := fmt.Sprintf("%d.dat", tsNano)
		sm.filePath = filepath.Join(sm.dbBasePath, sm.metricName, segmentFileName)
	}

	sm.currentBlockTimestamps = append(sm.currentBlockTimestamps, tsNano)
	sm.currentBlockValues = append(sm.currentBlockValues, val)
	sm.pointsInCurrentBlock++

	if sm.pointsInCurrentBlock == pointsPerBlock {
		err := sm.flushBlockAndReset()
		if err != nil {
			return fmt.Errorf("failed to flush block for metric %s: %w", sm.metricName, err)
		}
	}
	return nil
}

// flushBlockAndReset encodes and writes the current block to the segment file,
// then resets the block buffers.
// This function expects sm.mu to be held by the caller.
func (sm *SegmentManager) flushBlockAndReset() error {
	err := sm.flushBlock()
	if err != nil {
		return err
	}
	// Reset block buffers
	sm.currentBlockTimestamps = make([]int64, 0, pointsPerBlock)
	sm.currentBlockValues = make([]float64, 0, pointsPerBlock)
	sm.pointsInCurrentBlock = 0
	return nil
}

// flushBlock encodes and writes the current block to the segment file.
// This function expects sm.mu to be held by the caller.
// It does NOT reset the block buffers, allowing it to be used by Close().
func (sm *SegmentManager) flushBlock() error {
	if sm.pointsInCurrentBlock == 0 {
		return nil
	}

	if sm.file == nil {
		// sm.filePath should have been set by AddDataPoint when the first point for this block came in.
		if sm.filePath == "" {
			// This case should ideally not be reached if AddDataPoint correctly sets filePath.
			// Fallback: use the first timestamp in the current block.
			// This could happen if flush is called externally or during close before any data is added to a new segment.
			if len(sm.currentBlockTimestamps) == 0 {
				return fmt.Errorf("cannot determine segment filename for metric %s: no points in block and no file path set", sm.metricName)
			}
			segmentFileName := fmt.Sprintf("%d.dat", sm.currentBlockTimestamps[0])
			sm.filePath = filepath.Join(sm.dbBasePath, sm.metricName, segmentFileName)
		}

		// Ensure directory exists (it should, from DB.Write, but good to be safe)
		metricDir := filepath.Dir(sm.filePath)
		if err := os.MkdirAll(metricDir, 0755); err != nil {
			return fmt.Errorf("failed to create metric directory %s for segment file: %w", metricDir, err)
		}
		
		fmt.Printf("Opening segment file %s for metric %s\n", sm.filePath, sm.metricName) // Logging
		var err error
		sm.file, err = os.OpenFile(sm.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open segment file %s for metric %s: %w", sm.filePath, sm.metricName, err)
		}
	}

	// With the updated EncodeBlock, we can now correctly encode partial blocks.
	// The slices sm.currentBlockTimestamps[:sm.pointsInCurrentBlock] and
	// sm.currentBlockValues[:sm.pointsInCurrentBlock] will contain the exact
	// number of points to be written. EncodeBlock now includes a header with this count.
	fmt.Printf("Attempting to flush block for metric %s with %d points. File: %s\n", sm.metricName, sm.pointsInCurrentBlock, sm.filePath) // Logging

	pointsToEncode := sm.pointsInCurrentBlock
	timestampsToEncode := sm.currentBlockTimestamps[:pointsToEncode]
	valuesToEncode := sm.currentBlockValues[:pointsToEncode]

	encodedData, err := EncodeBlock(timestampsToEncode, valuesToEncode)
	if err != nil {
		return fmt.Errorf("failed to encode block for metric %s (points: %d): %w", sm.metricName, pointsToEncode, err)
	}

	if _, err := sm.file.Write(encodedData); err != nil {
		return fmt.Errorf("failed to write block to segment file %s for metric %s: %w", sm.filePath, sm.metricName, err)
	}

	if err := sm.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment file %s for metric %s: %w", sm.filePath, sm.metricName, err)
	}
	fmt.Printf("Successfully flushed block with %d points for metric %s to file %s\n", sm.pointsInCurrentBlock, sm.metricName, sm.filePath) // Logging
	return nil
}

// Helper to get a segment manager, creating it if necessary.
// This function expects db.mu to be held by the caller (DB's main mutex).
func (db *DB) getOrCreateSegmentManager(metricName string) (*SegmentManager, error) {
	sm, ok := db.openSegments[metricName]
	if !ok {
		metricPath := filepath.Join(db.basePath, metricName)
		if err := os.MkdirAll(metricPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metric directory %s: %w", metricPath, err)
		}
		fmt.Printf("Creating new segment manager for metric: %s\n", metricName) // Logging
		sm = &SegmentManager{
			metricName:           metricName,
			dbBasePath:           db.basePath,
			currentBlockTimestamps: make([]int64, 0, pointsPerBlock), // Initialize with capacity
			currentBlockValues:   make([]float64, 0, pointsPerBlock), // Initialize with capacity
			// filePath and file will be set when the first data point forces a flush/file creation
		}
		db.openSegments[metricName] = sm
	}
	return sm, nil
}

// NOTE on EncodeBlock and partial blocks:
// The previous NOTE is now resolved. EncodeBlock and DecodeBlock in storage.go
// have been updated to handle variable-sized blocks by including a point count
// in the block header. This ensures that partial blocks, such as those flushed
// during Close(), are correctly written and can be read back.

// segmentFileInfo holds information about a segment file.
type segmentFileInfo struct {
	filePath    string
	startTimestamp int64 // Unix nanoseconds
}

// Query retrieves data points for a given metric within a specified time range.
func (db *DB) Query(metricName string, queryStartTime time.Time, queryEndTime time.Time) ([]DataPoint, error) {
	if queryStartTime.After(queryEndTime) {
		return nil, fmt.Errorf("query start time cannot be after end time")
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	metricPath := filepath.Join(db.basePath, metricName)
	_, err := os.Stat(metricPath)
	if os.IsNotExist(err) {
		return []DataPoint{}, nil // Metric not found, return empty slice
	}
	if err != nil {
		return nil, fmt.Errorf("failed to stat metric directory %s: %w", metricPath, err)
	}

	dirEntries, err := os.ReadDir(metricPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metric directory %s: %w", metricPath, err)
	}

	var candidateSegments []segmentFileInfo
	queryStartNano := queryStartTime.UnixNano()
	queryEndNano := queryEndTime.UnixNano()

	for _, entry := range dirEntries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".dat" {
			continue
		}
		fileName := entry.Name()
		segmentStartStr := fileName[:len(fileName)-len(filepath.Ext(fileName))] // Remove .dat
		
		segmentStartTsNano, err := strconv.ParseInt(segmentStartStr, 10, 64)
		if err != nil {
			fmt.Printf("Warning: could not parse segment filename %s: %v. Skipping.\n", fileName, err)
			continue // Skip malformed filenames
		}

		// Pragmatic Segment File Selection:
		// A segment file <segment_start_ts_nano>.dat is a candidate if
		// segment_start_ts_nano < queryEndTime.UnixNano().
		// We also need to consider that points within this segment might span across queryStartTime.
		// A more aggressive filter would also check if segment_start_ts_nano + estimated_max_segment_duration > queryStartTime.UnixNano().
		// For now, this simpler filter is used, and block-level filtering will do the rest.
		// If a segment starts after the query ends, it's definitely not relevant.
		if segmentStartTsNano >= queryEndNano {
			continue
		}
		
		// Further optimization: if we knew the *end* timestamp of the segment (e.g., from an index),
		// we could also check `segment_end_ts_nano > queryStartTime.UnixNano()`.
		// Without it, we include segments that start before queryEnd and rely on point-level checks.

		candidateSegments = append(candidateSegments, segmentFileInfo{
			filePath:    filepath.Join(metricPath, fileName),
			startTimestamp: segmentStartTsNano,
		})
	}

	// Sort candidate segment files by their start timestamps in ascending order.
	sort.Slice(candidateSegments, func(i, j int) bool {
		return candidateSegments[i].startTimestamp < candidateSegments[j].startTimestamp
	})

	var results []DataPoint
	headerBuf := make([]byte, 2) // Buffer for reading the block header (numPoints)

	for _, segInfo := range candidateSegments {
		// Optimization check: If the segment starts *after* our query window has already been processed
		// by previous, earlier segments, we might be able to skip.
		// However, with out-of-order data, this is complex.
		// The current sort and iteration handles chronological processing of segments.

		file, err := os.Open(segInfo.filePath)
		if err != nil {
			fmt.Printf("Warning: could not open segment file %s: %v. Skipping.\n", segInfo.filePath, err)
			continue
		}
		defer file.Close() // Ensure file is closed

		fmt.Printf("Querying segment file: %s (starts at %d)\n", segInfo.filePath, segInfo.startTimestamp)

		for {
			// 1. Read block header (2 bytes for numPoints)
			n, err := io.ReadFull(file, headerBuf)
			if err == io.EOF { // Clean end of file, no more blocks
				break
			}
			if err == io.ErrUnexpectedEOF || (err != nil && err != io.EOF) { // Unexpected EOF or other read error
				fmt.Printf("Warning: error reading block header from %s (read %d bytes): %v. Skipping rest of file.\n", segInfo.filePath, n, err)
				break 
			}

			numPointsInBlock := int(binary.BigEndian.Uint16(headerBuf))
			if numPointsInBlock == 0 {
				continue // Skip empty blocks
			}
			
			if numPointsInBlock > pointsPerBlock {
                 fmt.Printf("Warning: block in %s claims to have %d points, which exceeds pointsPerBlock %d. Skipping rest of file.\n", segInfo.filePath, numPointsInBlock, pointsPerBlock)
                 break
            }


			// 2. Calculate data size and read block data
			blockDataSize := numPointsInBlock * (8 + 8) // 8 for timestamp, 8 for value
			blockData := make([]byte, blockDataSize)

			n, err = io.ReadFull(file, blockData)
			if err == io.EOF || err == io.ErrUnexpectedEOF { // EOF/UnexpectedEOF reading block data after header
				fmt.Printf("Warning: error reading block data from %s (expected %d bytes, got %d): %v. Skipping rest of file.\n", segInfo.filePath, blockDataSize, n, err)
				break
			}
			if err != nil {
				fmt.Printf("Warning: error reading block data from %s: %v. Skipping rest of file.\n", segInfo.filePath, err)
				break
			}
			
			// Prepend header to blockData for DecodeBlock
			fullBlockBytes := append(headerBuf, blockData...)

			// 3. Decode block
			timestamps, values, err := DecodeBlock(fullBlockBytes)
			if err != nil {
				fmt.Printf("Warning: could not decode block from %s: %v. Skipping block.\n", segInfo.filePath, err)
				continue
			}
			
			if len(timestamps) != numPointsInBlock { // Sanity check after decode
			    fmt.Printf("Warning: decoded points count %d does not match header count %d in %s. Skipping block.\n", len(timestamps), numPointsInBlock, segInfo.filePath)
			    continue
			}


			// 4. Iterate through decoded points and filter by query time range
			for i := 0; i < len(timestamps); i++ {
				tsNano := timestamps[i]
				if tsNano >= queryStartNano && tsNano < queryEndNano {
					results = append(results, DataPoint{
						MetricName: metricName,
						Timestamp:  time.Unix(0, tsNano),
						Value:      values[i],
					})
				}
			}
		}
		// file.Close() is handled by defer
	}

	// Results are not guaranteed to be sorted if points within blocks are not strictly sorted
	// or if segment files have overlapping time ranges not perfectly aligned with block boundaries.
	// However, our write path attempts to keep points in order within blocks and segments are processed in order.
	// For a truly robust sort, we'd sort `results` here if necessary.
	// sort.Slice(results, func(i, j int) bool {
	// 	return results[i].Timestamp.Before(results[j].Timestamp)
	// })
	// For now, assume order from files is sufficient.

	return results, nil
}

// Aggregate performs time-based aggregation on data points for a given metric.
func (db *DB) Aggregate(metricName string, queryStartTime time.Time, queryEndTime time.Time, interval time.Duration, aggFunc AggregationType) ([]AggregatedDataPoint, error) {
	// 1. Input Validation
	if queryStartTime.After(queryEndTime) {
		return nil, fmt.Errorf("query start time cannot be after end time")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("aggregation interval must be positive")
	}

	supportedAggFuncs := map[AggregationType]bool{
		AggSum: true, AggCount: true, AggMin: true, AggMax: true, AggAvg: true,
	}
	if !supportedAggFuncs[aggFunc] {
		return nil, fmt.Errorf("unsupported aggregation function: %s", aggFunc)
	}

	// 2. Fetch Raw Data
	rawDataPoints, err := db.Query(metricName, queryStartTime, queryEndTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query raw data for aggregation: %w", err)
	}
	if len(rawDataPoints) == 0 {
		return []AggregatedDataPoint{}, nil // No data to aggregate
	}

	// 3. Group Data by Interval
	// map[intervalStartTimeNano][]values
	groupedValues := make(map[int64][]float64)
	intervalNano := interval.Nanoseconds()

	for _, dp := range rawDataPoints {
		dpTsNano := dp.Timestamp.UnixNano()
		// Calculate the start of the interval this data point belongs to
		intervalStartNano := dpTsNano - (dpTsNano % intervalNano)
		groupedValues[intervalStartNano] = append(groupedValues[intervalStartNano], dp.Value)
	}

	// 4. Perform Aggregation per Interval
	var aggregatedResults []AggregatedDataPoint

	// Collect and sort interval start times to process in chronological order
	var intervalKeys []int64
	for k := range groupedValues {
		intervalKeys = append(intervalKeys, k)
	}
	sort.Slice(intervalKeys, func(i, j int) bool {
		return intervalKeys[i] < intervalKeys[j]
	})

	for _, intervalStartNano := range intervalKeys {
		valuesInInterval := groupedValues[intervalStartNano]
		var aggValue float64
		count := len(valuesInInterval)

		if count == 0 && (aggFunc == AggMin || aggFunc == AggMax || aggFunc == AggAvg || aggFunc == AggSum) {
			// This case should ideally not happen if rawDataPoints is not empty and grouping is correct.
			// If it does, for Min/Max, NaN is appropriate. For Sum, 0. For Avg, NaN. Count is 0.
			// However, if valuesInInterval is empty, the loops below won't run for Min/Max/Sum.
			// Let's handle specific cases.
			if aggFunc == AggCount {
				aggValue = 0
			} else {
				// For Sum, result is 0. For Min/Max/Avg of no values, NaN is a common representation.
				aggValue = math.NaN() 
				// Alternatively, could skip adding this interval or return an error,
				// but NaN is informative.
				if aggFunc == AggSum { // Sum of empty set is 0
				    aggValue = 0
				}
			}
			if aggFunc == AggCount { // Count is always defined
                 aggregatedResults = append(aggregatedResults, AggregatedDataPoint{
                    Timestamp:       time.Unix(0, intervalStartNano),
                    AggregatedValue: aggValue,
                })
                continue
            }
            // For other aggregations, if no values, we might skip or use NaN
            // For this implementation, if Min/Max/Avg/Sum and count is 0, we'll produce NaN (or 0 for Sum).
             if aggFunc != AggSum && aggFunc != AggMin && aggFunc != AggMax && aggFunc != AggAvg {
                 // Should not happen if count is 0 and not AggCount
                 fmt.Printf("Warning: Unhandled empty interval for aggFunc %s\n", aggFunc)
                 continue
             }

		}


		switch aggFunc {
		case AggSum:
			sum := 0.0
			for _, v := range valuesInInterval {
				sum += v
			}
			aggValue = sum
		case AggCount:
			aggValue = float64(count)
		case AggMin:
			if count == 0 { // Should be caught above, but defensive
				aggValue = math.NaN() // Or handle as error/skip
			} else {
				minVal := valuesInInterval[0]
				for i := 1; i < count; i++ {
					if valuesInInterval[i] < minVal {
						minVal = valuesInInterval[i]
					}
				}
				aggValue = minVal
			}
		case AggMax:
			if count == 0 { // Should be caught above
				aggValue = math.NaN()
			} else {
				maxVal := valuesInInterval[0]
				for i := 1; i < count; i++ {
					if valuesInInterval[i] > maxVal {
						maxVal = valuesInInterval[i]
					}
				}
				aggValue = maxVal
			}
		case AggAvg:
			if count == 0 { // Should be caught above
				aggValue = math.NaN() // Or return error: division by zero
			} else {
				sum := 0.0
				for _, v := range valuesInInterval {
					sum += v
				}
				aggValue = sum / float64(count)
			}
		default:
			// This case should have been caught by the initial validation
			return nil, fmt.Errorf("internal error: unsupported aggregation function %s reached processing", aggFunc)
		}
		
		// Only append if the value is not NaN, unless it's an AggCount of 0.
		// This handles cases where Min/Max/Avg on an empty set might result in NaN.
		// For count=0, AggCount is 0.0 which is valid. Sum of 0 is 0.0 which is valid.
		if !math.IsNaN(aggValue) || aggFunc == AggCount || aggFunc == AggSum {
			aggregatedResults = append(aggregatedResults, AggregatedDataPoint{
				Timestamp:       time.Unix(0, intervalStartNano),
				AggregatedValue: aggValue,
			})
		} else {
			// Optionally log that an interval produced NaN and was skipped
			fmt.Printf("Skipping interval at %d for metric %s due to NaN result (aggFunc: %s)\n", intervalStartNano, metricName, aggFunc)
		}
	}

	return aggregatedResults, nil
}

// DownsampleParams holds parameters for a downsampling operation.
type DownsampleParams struct {
	OriginalMetricName    string
	DownsampledMetricName string
	QueryStartTime        time.Time
	QueryEndTime          time.Time
	Interval              time.Duration
	AggFunc               AggregationType
}

// Downsample processes existing raw data for a given metric, computes aggregates
// over a specified downsampling interval, and stores this aggregated data
// as a new metric.
func (db *DB) Downsample(params DownsampleParams) error {
	// 1. Input Validation
	if params.OriginalMetricName == "" {
		return fmt.Errorf("original metric name cannot be empty")
	}
	if params.DownsampledMetricName == "" {
		return fmt.Errorf("downsampled metric name cannot be empty")
	}
	if params.OriginalMetricName == params.DownsampledMetricName {
		return fmt.Errorf("original and downsampled metric names cannot be the same")
	}
	if params.Interval <= 0 {
		return fmt.Errorf("downsampling interval must be positive")
	}
	if params.QueryStartTime.After(params.QueryEndTime) {
		return fmt.Errorf("query start time cannot be after end time")
	}

	supportedAggFuncs := map[AggregationType]bool{
		AggSum: true, AggCount: true, AggMin: true, AggMax: true, AggAvg: true,
	}
	if !supportedAggFuncs[params.AggFunc] {
		return fmt.Errorf("unsupported aggregation function: %s", params.AggFunc)
	}

	// 2. Fetch and Aggregate Data
	fmt.Printf("Downsampling: Fetching aggregated data for %s (start: %v, end: %v, interval: %v, agg: %s)\n",
		params.OriginalMetricName, params.QueryStartTime, params.QueryEndTime, params.Interval, params.AggFunc)

	aggregatedDataPoints, err := db.Aggregate(
		params.OriginalMetricName,
		params.QueryStartTime,
		params.QueryEndTime,
		params.Interval,
		params.AggFunc,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch and aggregate data for downsampling (metric: %s): %w", params.OriginalMetricName, err)
	}

	if len(aggregatedDataPoints) == 0 {
		fmt.Printf("Downsampling: No aggregated data points found for metric %s in the given range. Nothing to write to %s.\n",
			params.OriginalMetricName, params.DownsampledMetricName)
		return nil // Nothing to downsample
	}

	fmt.Printf("Downsampling: Found %d aggregated data points to write to %s.\n", len(aggregatedDataPoints), params.DownsampledMetricName)

	// 3. Write Downsampled Data
	for _, aggDP := range aggregatedDataPoints {
		// The aggDP.Timestamp is the start of the interval.
		// The aggDP.AggregatedValue is the value for that interval.
		err := db.Write(params.DownsampledMetricName, aggDP.Timestamp, aggDP.AggregatedValue)
		if err != nil {
			// If a write fails, stop and return the error.
			// More sophisticated error handling (e.g., retries, partial success tracking) is out of scope.
			return fmt.Errorf("failed to write downsampled data point to metric %s (timestamp: %v, value: %f): %w",
				params.DownsampledMetricName, aggDP.Timestamp, aggDP.AggregatedValue, err)
		}
	}

	fmt.Printf("Downsampling: Successfully wrote %d data points to %s.\n", len(aggregatedDataPoints), params.DownsampledMetricName)
	return nil
}
```
