package tstorage

// Partition is a chunk of time-series data with the timestamp range.
// A partition acts as a fully independent database containing all data
// points for its time range.
//
// The partition's lifecycle is: Writable -> ReadOnly.
// *Writable*:
//   it can be written. Only one partition can be writable within a partition list.
// *ReadOnly*:
//   it can't be written. Partitions will be ReadOnly if it exceeds the partition range.
type Partition interface {
	// Write operations
	//
	// InsertRows is a goroutine safe way to insert data points into itself.
	InsertRows(rows []Row) error

	// Read operations
	//
	// SelectRows gives back certain metric's data points within the given range.
	SelectRows(metricName string, start, end int64) []DataPoint
	// SelectAll gives back all rows of all metrics.
	SelectAll() []Row
	// MinTimestamp returns the minimum Unix timestamp in milliseconds.
	MinTimestamp() int64
	// MaxTimestamp returns the maximum Unix timestamp in milliseconds.
	MaxTimestamp() int64
	// Size returns the number of data points the partition holds.
	Size() int
	// ReadOnly indicates this partition is read only or not.
	ReadOnly() bool
}

type MemoryPartition interface {
	Partition
	ReadyToBePersisted() bool
}

type Row struct {
	DataPoint
	Labels []Label
}

type DataPoint struct {
	// Unix timestamp in milliseconds
	Timestamp int64
	Value     float64
}
