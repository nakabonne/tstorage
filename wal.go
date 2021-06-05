package tstorage

import (
	"os"
	"sync"
)

type walOperation byte

const (
	operationInsert walOperation = iota
)

// wal represents a write-ahead log, which offers durability guarantees.
// See more: https://martinfowler.com/articles/patterns-of-distributed-systems/wal.html
type wal interface {
	append(entry walEntry) error
}

// walEntry is an entry in the write-ahead log.
type walEntry struct {
	operation walOperation
	rows      []Row
}

type nopWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func (f *nopWAL) append(entry walEntry) error {
	// Do nothing
	return nil
}
