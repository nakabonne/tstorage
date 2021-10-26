package tstorage

import (
	"os"
	"sync"
)

type walOperation byte

const (
	// The record format for operateInsert is as shown below:
	/*
	   +--------+---------------------+--------+--------------------+----------------+
	   | op(1b) | len metric(varints) | metric | timestamp(varints) | value(varints) |
	   +--------+---------------------+--------+--------------------+----------------+
	*/
	operationInsert walOperation = iota
)

// wal represents a write-ahead log, which offers durability guarantees.
type wal interface {
	append(op walOperation, rows []Row) error
	flush() error
	punctuate() error
	truncateOldest() error
	removeAll() error
}

type nopWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func (f *nopWAL) append(_ walOperation, _ []Row) error {
	return nil
}

func (f *nopWAL) flush() error {
	return nil
}

func (f *nopWAL) punctuate() error {
	return nil
}

func (f *nopWAL) truncateOldest() error {
	return nil
}

func (f *nopWAL) removeAll() error {
	return nil
}
