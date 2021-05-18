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
	// minTimestamp returns the minimum Unix timestamp in milliseconds.
	minTimestamp() int64
	// maxTimestamp returns the maximum Unix timestamp in milliseconds.
	maxTimestamp() int64
	// size returns the number of data points the partition holds.
	size() int
	// active means not only writable but having the qualities to be the head partition.
	active() bool
}
