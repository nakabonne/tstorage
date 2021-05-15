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

func NewFileWAL(filename string) WAL {
	return &fileWAL{filename: filename}
}

func (f fileWAL) Append(entry Entry) error {
	// TODO: Implement appending to WAL.
	return nil
}
