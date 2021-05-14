package file

import (
	"os"
	"sync"

	"github.com/nakabonne/fiatdb/app/fiatd/wal"
)

type fileWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func NewFileWAL(filename string) wal.WAL {
	return &fileWAL{filename: filename}
}

func (f fileWAL) Append(entry wal.Entry) error {
	// TODO: Implement appending to WAL.
	return nil
}
