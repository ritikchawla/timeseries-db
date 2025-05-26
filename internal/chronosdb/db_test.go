package chronosdb

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
	"fmt"
	"math"
)

// Helper function to compare two DataPoint slices.
// Order matters for this simple helper.
func compareDataPoints(t *testing.T, expected, actual []DataPoint, checkMetricName bool) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("expected %d data points, got %d", len(expected), len(actual))
		// Log details of both slices for easier debugging
		t.Logf("Expected: %+v", expected)
		t.Logf("Actual:   %+v", actual)
		return
	}
	for i := range expected {
		if checkMetricName && expected[i].MetricName != actual[i].MetricName {
			t.Errorf("data point %d: expected metric name %s, got %s", i, expected[i].MetricName, actual[i].MetricName)
		}
		if !expected[i].Timestamp.Equal(actual[i].Timestamp) {
			t.Errorf("data point %d: expected timestamp %v, got %v", i, expected[i].Timestamp, actual[i].Timestamp)
		}
		// Handle float comparison carefully if necessary, but for exact writes/reads it should be fine.
		if expected[i].Value != actual[i].Value {
			t.Errorf("data point %d: expected value %f, got %f", i, expected[i].Value, actual[i].Value)
		}
	}
}

// Helper function to compare two AggregatedDataPoint slices.
// Order matters.
func compareAggregatedDataPoints(t *testing.T, expected, actual []AggregatedDataPoint) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("expected %d aggregated data points, got %d", len(expected), len(actual))
		t.Logf("Expected: %+v", expected)
		t.Logf("Actual:   %+v", actual)
		return
	}
	for i := range expected {
		if !expected[i].Timestamp.Equal(actual[i].Timestamp) {
			t.Errorf("agg data point %d: expected timestamp %v, got %v", i, expected[i].Timestamp, actual[i].Timestamp)
		}
		// Handle float comparison with tolerance for aggregated values
		if math.IsNaN(expected[i].AggregatedValue) && math.IsNaN(actual[i].AggregatedValue) {
			// Both are NaN, consider them equal in this context
		} else if math.Abs(expected[i].AggregatedValue-actual[i].AggregatedValue) > 1e-9 { // 1e-9 is a common tolerance
			t.Errorf("agg data point %d: expected value %f, got %f", i, expected[i].AggregatedValue, actual[i].AggregatedValue)
		}
	}
}


func TestDB_WriteAndQuery_Simple(t *testing.T) {
	tempDir := t.TempDir()
	// No need for os.RemoveAll(tempDir) when using t.TempDir()

	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	metricName := "test.metric.simple"
	now := time.Now().Truncate(time.Millisecond) // Truncate for cleaner timestamps

	pointsToWrite := []DataPoint{
		{MetricName: metricName, Timestamp: now.Add(1 * time.Second), Value: 10.5},
		{MetricName: metricName, Timestamp: now.Add(2 * time.Second), Value: 11.0},
		{MetricName: metricName, Timestamp: now.Add(3 * time.Second), Value: 10.0},
	}

	for _, p := range pointsToWrite {
		if err := db.Write(p.MetricName, p.Timestamp, p.Value); err != nil {
			t.Fatalf("Write failed for %s at %v: %v", p.MetricName, p.Timestamp, err)
		}
	}

	// Close and reopen to test persistence
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() failed: %v", err)
	}
	db, err = NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}
	defer db.Close() // Ensure this instance is also closed

	// 1. Query entire range
	retrievedPoints, err := db.Query(metricName, now, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("Query failed for entire range: %v", err)
	}
	compareDataPoints(t, pointsToWrite, retrievedPoints, false) // MetricName is implicit in query

	// 2. Query a sub-range
	subRangePoints, err := db.Query(metricName, now.Add(1500*time.Millisecond), now.Add(2500*time.Millisecond))
	if err != nil {
		t.Fatalf("Query failed for sub-range: %v", err)
	}
	expectedSubRange := []DataPoint{pointsToWrite[1]} // Only the point at 2s
	compareDataPoints(t, expectedSubRange, subRangePoints, false)

	// 3. Query a time range with no data (before all points)
	noDataPointsBefore, err := db.Query(metricName, now.Add(-10*time.Second), now.Add(-5*time.Second))
	if err != nil {
		t.Fatalf("Query failed for no data range (before): %v", err)
	}
	if len(noDataPointsBefore) != 0 {
		t.Errorf("expected 0 points for query before data, got %d", len(noDataPointsBefore))
	}

	// 4. Query a time range with no data (after all points)
	noDataPointsAfter, err := db.Query(metricName, now.Add(10*time.Second), now.Add(15*time.Second))
	if err != nil {
		t.Fatalf("Query failed for no data range (after): %v", err)
	}
	if len(noDataPointsAfter) != 0 {
		t.Errorf("expected 0 points for query after data, got %d", len(noDataPointsAfter))
	}
	
	// 5. Query a non-existent metric
	nonExistentMetric := "does.not.exist"
	noMetricPoints, err := db.Query(nonExistentMetric, now, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("Query failed for non-existent metric: %v", err)
	}
	if len(noMetricPoints) != 0 {
		t.Errorf("expected 0 points for non-existent metric, got %d", len(noMetricPoints))
	}
}

func TestDB_Write_MultipleBlocks(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	metricName := "test.metric.multiblock"
	numPointsToWrite := int(float64(pointsPerBlock) * 2.5) // e.g., 1024 * 2.5 = 2560
	now := time.Now().Truncate(time.Second)

	var pointsToWrite []DataPoint
	for i := 0; i < numPointsToWrite; i++ {
		pointsToWrite = append(pointsToWrite, DataPoint{
			MetricName: metricName,
			Timestamp:  now.Add(time.Duration(i) * time.Second),
			Value:      float64(i),
		})
	}

	for _, p := range pointsToWrite {
		if err := db.Write(p.MetricName, p.Timestamp, p.Value); err != nil {
			t.Fatalf("Write failed for %s at %v: %v", p.MetricName, p.Timestamp, err)
		}
	}

	// Close and reopen
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() failed: %v", err)
	}
	db, err = NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}
	defer db.Close()

	retrievedPoints, err := db.Query(metricName, now, now.Add(time.Duration(numPointsToWrite)*time.Second))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	compareDataPoints(t, pointsToWrite, retrievedPoints, false)
}

func TestDB_Write_PartialBlockFlushOnClose(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	metricName := "test.metric.partialflush"
	numPointsToWrite := 10
	now := time.Now().Truncate(time.Second)

	var pointsToWrite []DataPoint
	for i := 0; i < numPointsToWrite; i++ {
		pointsToWrite = append(pointsToWrite, DataPoint{
			MetricName: metricName,
			Timestamp:  now.Add(time.Duration(i) * time.Second),
			Value:      float64(i) * 0.1,
		})
	}

	for _, p := range pointsToWrite {
		if err := db.Write(p.MetricName, p.Timestamp, p.Value); err != nil {
			t.Fatalf("Write failed for %s at %v: %v", p.MetricName, p.Timestamp, err)
		}
	}

	// Close (this should trigger flush of the partial block)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() failed: %v", err)
	}

	// Reopen
	db, err = NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}
	defer db.Close()

	retrievedPoints, err := db.Query(metricName, now, now.Add(time.Duration(numPointsToWrite)*time.Second))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	compareDataPoints(t, pointsToWrite, retrievedPoints, false)
}

// TestDB_Query_SegmentBoundaries is simplified due to the complexity of
// reliably creating multiple segments with "first timestamp names the segment" logic
// without knowing the exact segment duration/rollover logic if it were more complex.
// This test will focus on queries within a single segment that has multiple blocks.
// A more advanced version would require manual creation of segment files or more control.
func TestDB_Query_SingleSegmentMultipleBlocks(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	metricName := "test.metric.singlesegment.multiblock"
	// Write enough points for roughly 2 blocks, all within one segment
	numPointsToWrite := pointsPerBlock + pointsPerBlock/2 
	now := time.Now().Truncate(time.Second)
	
	var pointsToWrite []DataPoint
	for i := 0; i < numPointsToWrite; i++ {
		// Ensure all timestamps are close enough to likely stay in one segment file
		// if segment rollover was based on large time gaps. Given current logic,
		// all these points will be in the same segment file named by the first point's timestamp.
		ts := now.Add(time.Duration(i) * 10 * time.Millisecond) // Small time increments
		pointsToWrite = append(pointsToWrite, DataPoint{
			MetricName: metricName,
			Timestamp:  ts,
			Value:      float64(i),
		})
		if err := db.Write(metricName, ts, float64(i)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	
	// No close & reopen here, query directly to test in-memory state if some data is still buffered
	// or after implicit flushes due to full blocks.

	// Query parts of the data
	// 1. Query first block worth of data
	queryEnd1 := now.Add(time.Duration(pointsPerBlock) * 10 * time.Millisecond)
	retrieved1, err := db.Query(metricName, now, queryEnd1)
	if err != nil {
		t.Fatalf("Query for first block failed: %v", err)
	}
	// Extract the expected points for the first block
	var expected1 []DataPoint
	for _, p := range pointsToWrite {
		if p.Timestamp.Before(queryEnd1) {
			expected1 = append(expected1, p)
		}
	}
	compareDataPoints(t, expected1, retrieved1, false)

	// 2. Query across block boundary
	queryStart2 := now.Add(time.Duration(pointsPerBlock-5) * 10 * time.Millisecond)
	queryEnd2 := now.Add(time.Duration(pointsPerBlock+5) * 10 * time.Millisecond)
	retrieved2, err := db.Query(metricName, queryStart2, queryEnd2)
	if err != nil {
		t.Fatalf("Query across block boundary failed: %v", err)
	}
	var expected2 []DataPoint
	for _, p := range pointsToWrite {
		if !p.Timestamp.Before(queryStart2) && p.Timestamp.Before(queryEnd2) {
			expected2 = append(expected2, p)
		}
	}
	compareDataPoints(t, expected2, retrieved2, false)
	
	// 3. Query all data
	queryEndAll := now.Add(time.Duration(numPointsToWrite) * 10 * time.Millisecond)
	retrievedAll, err := db.Query(metricName, now, queryEndAll)
	if err != nil {
		t.Fatalf("Query for all data failed: %v", err)
	}
	compareDataPoints(t, pointsToWrite, retrievedAll, false)
}


func TestDB_Aggregate_Simple(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	metricName := "test.metric.aggregate"
	// Timestamps are Unix seconds for simplicity in manual calculation
	// Values: 10, 20, 15, 25, 30 (total 5 points)
	// Ts:     0,  1,  2,  3,  4  (relative to 'baseTime')
	// Interval: 2s
	// Window 1 (0-1.999s): points at 0s (10), 1s (20)
	// Window 2 (2-3.999s): points at 2s (15), 3s (25)
	// Window 3 (4-5.999s): points at 4s (30)
	baseTime := time.Unix(1678886400, 0) // A fixed base time

	points := []struct {
		Offset time.Duration
		Value  float64
	}{
		{0 * time.Second, 10.0},
		{1 * time.Second, 20.0},
		{2 * time.Second, 15.0},
		{3 * time.Second, 25.0},
		{4 * time.Second, 30.0},
	}

	for _, p := range points {
		if err := db.Write(metricName, baseTime.Add(p.Offset), p.Value); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	interval := 2 * time.Second
	startTime := baseTime
	endTime := baseTime.Add(5 * time.Second) // Covers all points

	// Sum
	expectedSum := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 30.0},                            // 10+20
		{Timestamp: baseTime.Add(2 * time.Second), AggregatedValue: 40.0},        // 15+25
		{Timestamp: baseTime.Add(4 * time.Second), AggregatedValue: 30.0},        // 30
	}
	actualSum, err := db.Aggregate(metricName, startTime, endTime, interval, AggSum)
	if err != nil {
		t.Fatalf("Aggregate(Sum) failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedSum, actualSum)

	// Count
	expectedCount := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 2.0},
		{Timestamp: baseTime.Add(2 * time.Second), AggregatedValue: 2.0},
		{Timestamp: baseTime.Add(4 * time.Second), AggregatedValue: 1.0},
	}
	actualCount, err := db.Aggregate(metricName, startTime, endTime, interval, AggCount)
	if err != nil {
		t.Fatalf("Aggregate(Count) failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedCount, actualCount)

	// Min
	expectedMin := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 10.0},
		{Timestamp: baseTime.Add(2 * time.Second), AggregatedValue: 15.0},
		{Timestamp: baseTime.Add(4 * time.Second), AggregatedValue: 30.0},
	}
	actualMin, err := db.Aggregate(metricName, startTime, endTime, interval, AggMin)
	if err != nil {
		t.Fatalf("Aggregate(Min) failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedMin, actualMin)

	// Max
	expectedMax := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 20.0},
		{Timestamp: baseTime.Add(2 * time.Second), AggregatedValue: 25.0},
		{Timestamp: baseTime.Add(4 * time.Second), AggregatedValue: 30.0},
	}
	actualMax, err := db.Aggregate(metricName, startTime, endTime, interval, AggMax)
	if err != nil {
		t.Fatalf("Aggregate(Max) failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedMax, actualMax)

	// Avg
	expectedAvg := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 15.0},                            // (10+20)/2
		{Timestamp: baseTime.Add(2 * time.Second), AggregatedValue: 20.0},        // (15+25)/2
		{Timestamp: baseTime.Add(4 * time.Second), AggregatedValue: 30.0},        // 30/1
	}
	actualAvg, err := db.Aggregate(metricName, startTime, endTime, interval, AggAvg)
	if err != nil {
		t.Fatalf("Aggregate(Avg) failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedAvg, actualAvg)
	
	// Test with an interval that covers all points in one window
	intervalAll := 5 * time.Second
	expectedSumAll := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 10.0 + 20.0 + 15.0 + 25.0 + 30.0}, // Sum of all
	}
	actualSumAll, err := db.Aggregate(metricName, startTime, endTime, intervalAll, AggSum)
	if err != nil {
		t.Fatalf("Aggregate(Sum) with large interval failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedSumAll, actualSumAll)
}


func TestDB_Aggregate_EmptyResult(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	metricName := "test.metric.agg.empty"
	nonExistentMetric := "does.not.exist.agg"
	baseTime := time.Unix(1678886400, 0)
	interval := 1 * time.Minute

	// 1. Metric with no data
	aggResult, err := db.Aggregate(nonExistentMetric, baseTime, baseTime.Add(1*time.Hour), interval, AggSum)
	if err != nil {
		t.Fatalf("Aggregate on non-existent metric failed: %v", err)
	}
	if len(aggResult) != 0 {
		t.Errorf("expected 0 aggregated points for non-existent metric, got %d", len(aggResult))
	}

	// 2. Metric with data, but time range with no data
	if err := db.Write(metricName, baseTime.Add(2*time.Hour), 10.0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	aggResultNoDataInRange, err := db.Aggregate(metricName, baseTime, baseTime.Add(1*time.Hour), interval, AggSum)
	if err != nil {
		t.Fatalf("Aggregate on time range with no data failed: %v", err)
	}
	if len(aggResultNoDataInRange) != 0 {
		t.Errorf("expected 0 aggregated points for time range with no data, got %d", len(aggResultNoDataInRange))
	}
}

func TestDB_Aggregate_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	metricName := "test.metric.agg.edge"
	baseTime := time.Unix(1678886400, 0) // Start of an exact minute

	// Single point in an interval
	if err := db.Write(metricName, baseTime.Add(30*time.Second), 50.0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	interval := 1 * time.Minute
	startTime := baseTime
	endTime := baseTime.Add(1 * time.Minute)

	// Avg with single point
	expectedAvg := []AggregatedDataPoint{{Timestamp: baseTime, AggregatedValue: 50.0}}
	actualAvg, err := db.Aggregate(metricName, startTime, endTime, interval, AggAvg)
	if err != nil {
		t.Fatalf("Aggregate(Avg) single point failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedAvg, actualAvg)

	// Min with single point
	expectedMin := []AggregatedDataPoint{{Timestamp: baseTime, AggregatedValue: 50.0}}
	actualMin, err := db.Aggregate(metricName, startTime, endTime, interval, AggMin)
	if err != nil {
		t.Fatalf("Aggregate(Min) single point failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedMin, actualMin)

	// Max with single point
	expectedMax := []AggregatedDataPoint{{Timestamp: baseTime, AggregatedValue: 50.0}}
	actualMax, err := db.Aggregate(metricName, startTime, endTime, interval, AggMax)
	if err != nil {
		t.Fatalf("Aggregate(Max) single point failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedMax, actualMax)
	
	// Aggregation where query range does not perfectly align
	// Points: 10s -> 100, 70s -> 200. Interval 1min.
	// Query: 5s to 65s.
	// Interval 1: 0s-59s. Point: 10s (100)
	// Interval 2: 60s-119s. Point: 70s (200) -> this interval starts at 60s.
	// Query range is 5s to 65s.
	// Raw data point at 10s (value 100) is in range. Interval starts at 0s.
	// Raw data point at 70s (value 200) is NOT in query range (5s-65s).
	if err := db.Write(metricName, baseTime.Add(10*time.Second), 100.0); err != nil { // Overwrites previous data, different metric name used implicitly by test structure if run in parallel
		t.Fatalf("Write failed: %v", err)
	}
	if err := db.Write(metricName, baseTime.Add(70*time.Second), 200.0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	startTimeMisaligned := baseTime.Add(5 * time.Second)
	endTimeMisaligned := baseTime.Add(65 * time.Second) // Query ends at 65s.
	
	// Interval 1 (starts at baseTime, 0s): point at 10s (100)
	// Interval 2 (starts at baseTime.Add(1*time.Minute), 60s): point at 70s (200)
	// Query is [5s, 65s).
	// Raw point 10s is in query range. It belongs to interval starting at 0s.
	// Raw point 70s is NOT in query range.
	// So, only the first interval (0s) should have data based on the query.
	expectedMisaligned := []AggregatedDataPoint{
		{Timestamp: baseTime, AggregatedValue: 100.0}, // Only data from point at 10s
	}
	actualMisaligned, err := db.Aggregate(metricName, startTimeMisaligned, endTimeMisaligned, interval, AggAvg)
	if err != nil {
		t.Fatalf("Aggregate with misaligned query range failed: %v", err)
	}
	compareAggregatedDataPoints(t, expectedMisaligned, actualMisaligned)
}


func TestDB_Downsample_Simple(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	originalMetric := "test.metric.raw"
	downsampledMetric := "test.metric.downsampled.1min_avg"
	baseTime := time.Unix(1678886400, 0) // Start of an exact minute

	// Raw data:
	// 00s: 10
	// 10s: 20
	// 70s: 60 (next interval)
	// 80s: 70 (next interval)
	points := []struct{ts time.Time; val float64}{
		{baseTime.Add(0 * time.Second), 10.0},
		{baseTime.Add(10 * time.Second), 20.0},
		{baseTime.Add(70 * time.Second), 60.0},
		{baseTime.Add(80 * time.Second), 70.0},
	}
	for _, p := range points {
		if err := db.Write(originalMetric, p.ts, p.val); err != nil {
			t.Fatalf("Write to originalMetric failed: %v", err)
		}
	}

	params := DownsampleParams{
		OriginalMetricName:    originalMetric,
		DownsampledMetricName: downsampledMetric,
		QueryStartTime:        baseTime,
		QueryEndTime:          baseTime.Add(2 * time.Minute), // Cover both intervals
		Interval:              1 * time.Minute,
		AggFunc:               AggAvg,
	}

	if err := db.Downsample(params); err != nil {
		t.Fatalf("Downsample failed: %v", err)
	}

	// Expected downsampled data:
	// Interval 1 (starts at baseTime): Avg(10, 20) = 15
	// Interval 2 (starts at baseTime.Add(1 min)): Avg(60, 70) = 65
	expectedDownsampled := []DataPoint{
		{MetricName: downsampledMetric, Timestamp: baseTime, Value: 15.0},
		{MetricName: downsampledMetric, Timestamp: baseTime.Add(1 * time.Minute), Value: 65.0},
	}

	// Query the downsampled metric
	retrievedDownsampled, err := db.Query(downsampledMetric, baseTime, baseTime.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("Query on downsampledMetric failed: %v", err)
	}
	
	// Sort both slices by timestamp before comparing, as query results might not be strictly ordered
    // if segment files are named by first point and downsampling involves multiple writes.
    // However, our Write path for downsampled data should be sequential for timestamps.
    // For robustness, sorting is good.
	sort.Slice(expectedDownsampled, func(i, j int) bool { return expectedDownsampled[i].Timestamp.Before(expectedDownsampled[j].Timestamp) })
	sort.Slice(retrievedDownsampled, func(i, j int) bool { return retrievedDownsampled[i].Timestamp.Before(retrievedDownsampled[j].Timestamp) })
	
	compareDataPoints(t, expectedDownsampled, retrievedDownsampled, true) // Check metric name here
}

func TestDB_Write_MetricNames(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDB(tempDir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now()
	metricsToTest := []string{
		"cpu.load",
		"memory_usage_percent",
		"network.interface.eth0.bytes_rx",
		// "metric-with-hyphens", // Filepath separators might be an issue depending on OS, but ChronosDB uses metric name as dir name.
		// "metric.with.dots.많음", // Non-ASCII, could be problematic for file systems.
	}

	for _, metricName := range metricsToTest {
		t.Run(metricName, func(t *testing.T) {
			dpWrite := DataPoint{MetricName: metricName, Timestamp: now, Value: 123.45}
			if err := db.Write(dpWrite.MetricName, dpWrite.Timestamp, dpWrite.Value); err != nil {
				t.Fatalf("Write failed for metric '%s': %v", metricName, err)
			}

			// Query back - simple check
			retrieved, err := db.Query(metricName, now.Add(-1*time.Second), now.Add(1*time.Second))
			if err != nil {
				t.Fatalf("Query failed for metric '%s': %v", metricName, err)
			}
			if len(retrieved) != 1 {
				t.Fatalf("Expected 1 point for metric '%s', got %d", metricName, len(retrieved))
			}
			if retrieved[0].Value != dpWrite.Value || !retrieved[0].Timestamp.Equal(dpWrite.Timestamp) {
				t.Errorf("Retrieved point %+v does not match written point %+v for metric '%s'", retrieved[0], dpWrite, metricName)
			}
		})
	}
	// Note: This test doesn't close and reopen the DB between sub-tests for metric names.
	// It primarily checks if the write/query path handles these names without immediate error.
	// The directory creation from metric names is a key aspect.
}

// TODO: TestDB_Query_SegmentBoundaries - needs more thought on reliable segment creation for testing.
// The current simplified version `TestDB_Query_SingleSegmentMultipleBlocks` covers multi-block reads.
// True segment boundary tests would ideally involve creating segment files manually or having a
// deterministic way to force segment splits (e.g. if segments were time-duration based like "1 hour per file").
// With "first timestamp in block names the file", writing points with significantly different timestamps
// that are also the *first* point in their respective blocks would create new files.

```
