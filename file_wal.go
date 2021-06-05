package tstorage

import (
	"fmt"
	"os"
	"sync"
)

type fileWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func newFileWal(filename string) (wal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	return &fileWAL{
		filename: filename,
		f:        f,
	}, nil
}

// append appends the given entry to the end of a file via the file descriptor it has.
func (w fileWAL) append(entry walEntry) error {
	if w.f == nil {
		return fmt.Errorf("no file descriptor")
	}
	// TODO: Implement appending to wal.
	return nil
}
