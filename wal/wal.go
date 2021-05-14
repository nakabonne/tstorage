package wal

import "github.com/nakabonne/fiatdb/app/fiatd/storage/partition"

type Operation byte

const (
	OperationInsert Operation = iota
)

// WAL is a write-ahead log.
// See more: https://martinfowler.com/articles/patterns-of-distributed-systems/wal.html
type WAL interface {
	Append(entry Entry) error
}

// Entry is an entry in the write-ahead log.
type Entry struct {
	Operation Operation
	Rows      []partition.Row
}
