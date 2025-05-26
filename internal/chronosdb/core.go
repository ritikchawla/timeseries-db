package chronosdb

import "time"

// DataPoint represents a single data point for a metric.
type DataPoint struct {
	MetricName string
	Timestamp  time.Time
	Value      float64
}

// AggregatedDataPoint represents a single aggregated data point.
type AggregatedDataPoint struct {
	Timestamp       time.Time // Start of the aggregation interval
	AggregatedValue float64
}

// Core database logic will go here.
// For now, this is a placeholder.
type DB struct {
	// Placeholder for database fields
}
