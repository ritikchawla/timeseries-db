package chronosdb

import (
	"encoding/binary"
	"fmt"
	"math"
)

// pointsPerBlock defines the number of data points within a single block.
const pointsPerBlock = 1024

// Block represents an in-memory version of a block of data.
// It holds slices of timestamps and values for pointsPerBlock DataPoints.
type Block struct {
	Timestamps []int64
	Values     []float64
}

// NewBlock creates a new Block, pre-allocating space for timestamps and values.
func NewBlock() *Block {
	return &Block{
		Timestamps: make([]int64, 0, pointsPerBlock),
		Values:     make([]float64, 0, pointsPerBlock),
	}
}

// EncodeBlock serializes a block of timestamps and values into a single []byte slice.
// The format is:
// - Header (2 bytes): Number of points in this block (uint16, BigEndian).
// - Timestamps: numPoints * 8 bytes (int64, BigEndian).
// - Values: numPoints * 8 bytes (float64, BigEndian).
// Assumes that len(timestamps) == len(values).
func EncodeBlock(timestamps []int64, values []float64) ([]byte, error) {
	numPoints := len(timestamps)
	if numPoints != len(values) {
		return nil, fmt.Errorf("mismatch between number of timestamps (%d) and values (%d)", numPoints, len(values))
	}
	if numPoints > pointsPerBlock { // Ensure we don't exceed overall block capacity; pointsPerBlock is a logical limit.
		return nil, fmt.Errorf("number of points %d exceeds pointsPerBlock %d", numPoints, pointsPerBlock)
	}
	if numPoints == 0 {
		// Handle empty block case: encode as 0 points, return empty data or just header.
		// For simplicity, let's say an empty block is just a header with 0 points.
		headerSizeBytes := 2
		buf := make([]byte, headerSizeBytes)
		binary.BigEndian.PutUint16(buf[0:2], uint16(0))
		return buf, nil
	}

	headerSizeBytes := 2                                     // For uint16 point count
	dataSizeBytes := numPoints * (8 + 8)                     // 8 bytes for timestamp, 8 bytes for value
	totalSizeBytes := headerSizeBytes + dataSizeBytes
	buf := make([]byte, totalSizeBytes)

	// Write header: number of points
	binary.BigEndian.PutUint16(buf[0:2], uint16(numPoints))

	offset := headerSizeBytes
	// Write all timestamps
	for _, ts := range timestamps {
		binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(ts))
		offset += 8
	}

	// Write all values
	for _, val := range values {
		binary.BigEndian.PutUint64(buf[offset:offset+8], math.Float64bits(val))
		offset += 8
	}

	return buf, nil
}

// DecodeBlock deserializes a []byte slice (representing a serialized block)
// back into slices of timestamps and values.
// The block is expected to have a 2-byte header (uint16, BigEndian) indicating the number of points.
func DecodeBlock(data []byte) (timestamps []int64, values []float64, err error) {
	headerSizeBytes := 2 // For uint16 point count
	if len(data) < headerSizeBytes {
		return nil, nil, fmt.Errorf("input data length %d is less than header size %d", len(data), headerSizeBytes)
	}

	numPoints := int(binary.BigEndian.Uint16(data[0:headerSizeBytes]))

	if numPoints == 0 {
		// If numPoints is 0, check if data length is exactly header size
		if len(data) != headerSizeBytes {
			return nil, nil, fmt.Errorf("input data length %d for 0 points does not match expected header size %d", len(data), headerSizeBytes)
		}
		return []int64{}, []float64{}, nil // Return empty slices for 0 points
	}
	
	expectedDataSizeBytes := numPoints * (8 + 8) // 8 for timestamp, 8 for value
	expectedTotalSizeBytes := headerSizeBytes + expectedDataSizeBytes

	if len(data) != expectedTotalSizeBytes {
		return nil, nil, fmt.Errorf("input data length %d does not match expected total length %d for %d points", len(data), expectedTotalSizeBytes, numPoints)
	}

	timestamps = make([]int64, numPoints)
	values = make([]float64, numPoints)

	offset := headerSizeBytes
	// Read all timestamps
	for i := 0; i < numPoints; i++ {
		timestamps[i] = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	// Read all values
	for i := 0; i < numPoints; i++ {
		values[i] = math.Float64frombits(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	return timestamps, values, nil
}

// This file outlines the initial design for ChronosDB's storage format.
// Actual implementation will follow in subsequent tasks.

// **Binary Format for a Single DataPoint (within a data block):**
//
// We will store DataPoints for the same metric together.
// The MetricName is implied by the directory structure (see Data Segmentation).
//
// 1. Timestamp:
//    - Type: int64 (Unix nanoseconds)
//    - Encoding: BigEndian
//    - Size: 8 bytes
//
// 2. Value:
//    - Type: float64
//    - Encoding: BigEndian, using math.Float64bits() to convert to uint64 then to bytes.
//    - Size: 8 bytes
//
// Total size for one DataPoint (Timestamp + Value) = 16 bytes.

// **Data Segmentation:**
//
// 1. Base Directory:
//    - A configurable base data directory (e.g., /var/lib/chronosdb/data).
//
// 2. Metric-Specific Directories:
//    - Data for each metric will be stored in its own subdirectory.
//    - Path: <base_data_dir>/<metric_name>/
//    - Example: /var/lib/chronosdb/data/cpu.load_avg.1m/
//
// 3. Segment Files (Time Window based):
//    - Within each metric's directory, data will be segmented into files.
//    - Each file represents a fixed time window (e.g., 1 hour or 1 day).
//      The exact duration will be configurable. For this initial design, let's assume 1 day.
//    - Filename Format: <unix_timestamp_start_of_window_ns>.dat
//    - Example: /var/lib/chronosdb/data/cpu.load_avg.1m/1678886400000000000.dat (representing 2023-03-15 00:00:00 UTC)

// **Block-Based Structure within a Segment File:**
//
// 1. Purpose:
//    - To improve read/write efficiency by processing data in chunks.
//    - To facilitate potential compression per block in the future.
//    - To allow for easier indexing and seeking.
//
// 2. Block Structure:
//    - Each segment file will consist of one or more blocks.
//    - A block will contain a fixed number of DataPoints (e.g., 1024 points) or have a fixed byte size (e.g., 64KB).
//      Let's initially propose a fixed number of points, e.g., 1024 DataPoints.
//      This means a raw data block would be 1024 * 16 bytes = 16KB.
//
// 3. Columnar Approach within a Block:
//    - To optimize for queries that only need timestamps or only values, and to potentially improve compression ratios.
//
//    - Option 1 (Preferred for simplicity and good compression potential):
//      - Store all timestamps for the block consecutively, followed by all values for the block.
//      - [Timestamp1, Timestamp2, ..., TimestampN][Value1, Value2, ..., ValueN]
//      - Example (for a block of 1024 points):
//        - First 1024 * 8 bytes: Timestamps section (all int64, BigEndian)
//        - Next 1024 * 8 bytes: Values section (all float64, BigEndian)
//
//    - Option 2 (Interleaved, less ideal for typical TSDB queries):
//      - [Timestamp1, Value1, Timestamp2, Value2, ..., TimestampN, ValueN]
//      - This is simpler to write sequentially but less efficient for columnar access patterns.
//
// 4. Block Header (Optional but Recommended for future enhancements):
//    - We might prefix each block with a small header.
//    - Contents could include:
//      - Block version number (1 byte)
//      - Compression type used for this block (1 byte, e.g., 0=none, 1=snappy, 2=zstd)
//      - Checksum for the block data (e.g., CRC32, 4 bytes)
//      - Count of DataPoints in this block (if variable, e.g., for the last block in a segment)
//
// **Index Files (Conceptual - for later implementation):**
//
// - To efficiently locate segment files and blocks within segment files.
// - Per-metric index: <base_data_dir>/<metric_name>/index.idx
// - This index might store:
//   - For each segment file:
//     - Start Timestamp of the segment
//     - Offset to the start of the segment file (or just derive from filename)
//     - Potentially, a sparse index of (Timestamp, BlockOffset) pairs within the segment file.

// **Aggregated Data Storage (Conceptual - for later implementation):**
//
// - Aggregated data (e.g., hourly rollups from 1-minute data) could be stored:
//   - In separate directories: <base_data_dir>/<metric_name>_aggregated_<interval>/
//   - Using a similar segment and block structure as raw data.
//   - The `AggregatedDataPoint` struct would be used, and its binary format would be:
//     - Timestamp (start of interval): int64, BigEndian (8 bytes)
//     - AggregatedValue: float64, BigEndian (8 bytes)
//     - Total: 16 bytes per AggregatedDataPoint.
//
// This initial design focuses on simplicity and efficiency for common time-series workloads.
// Further refinements and details (like compression, indexing, and concurrency control)
// will be addressed in subsequent development phases.
