package tstorage

import (
	"os"
	"sync"
)

type fileWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func newFileWal(filename string) wal {
	return &fileWAL{filename: filename}
}

func (f fileWAL) append(entry walEntry) error {
	// TODO: Implement appending to wal.
	return nil
}
