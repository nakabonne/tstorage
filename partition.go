package tstorage

// partition is a chunk of time-series data with the timestamp range.
// A partition acts as a fully independent database containing all data
// points for its time range.
//
// The partition's lifecycle is: Writable -> ReadOnly.
// *Writable*:
//   it can be written. Only one partition can be writable within a partition list.
// *ReadOnly*:
//   it can't be written. Partitions will be ReadOnly if it exceeds the partition range.
// FIXME: Make public methods private
type partition interface {
	// Write operations
	//
	// insertRows is a goroutine safe way to insert data points into itself.
	insertRows(rows []Row) error

	// Read operations
	//
	// selectRows gives back certain metric's data points within the given range.
	selectRows(metric string, labels []Label, start, end int64) dataPointList
	// selectAll gives back all rows of all metrics.
	selectAll() []Row
	// MinTimestamp returns the minimum Unix timestamp in milliseconds.
	MinTimestamp() int64
	// MaxTimestamp returns the maximum Unix timestamp in milliseconds.
	MaxTimestamp() int64
	// Size returns the number of data points the partition holds.
	Size() int
	// ReadOnly indicates this partition is read only or not.
	ReadOnly() bool
}

type inMemoryPartition interface {
	partition
	ReadyToBePersisted() bool
}
