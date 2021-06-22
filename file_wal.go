package tstorage

import (
	"fmt"
	"os"
	"sync"
)

var (
	// Magic sequence to check for valid data.
	walMagic = uint32(0x11141993)
)

type fileWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func newFileWal(filename string) (wal, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	// TODO: Implement appending to wal correctly.

	/*
		if w.f == nil {
			return fmt.Errorf("no file descriptor")
		}

		// Buffer writes until the end.
		buf := &bytes.Buffer{}
		var err error

		// Write the operation type
		if err = buf.WriteByte(byte(entry.operation)); err != nil {
			return err
		}

		for _, row := range entry.rows {
			// Write metric name length
			name := marshalMetricName(row.Metric, row.Labels)
			if _, err := buf.WriteString(name); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.LittleEndian, row.DataPoint.Timestamp); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.LittleEndian, row.DataPoint.Value); err != nil {
				return err
			}
		}

		w.mu.Lock()
		defer w.mu.Unlock()

		// Flush to the file.
		if _, err := w.f.Write(buf.Bytes()); err != nil {
			return err
		}
	*/
	return nil
}
