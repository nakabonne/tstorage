package tstorage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

type fileWAL struct {
	filename     string
	w            *bufio.Writer
	bufferedSize int
	mu           sync.Mutex
}

func newFileWal(filename string, bufferedSize int) (wal, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	return &fileWAL{
		filename:     filename,
		w:            bufio.NewWriterSize(f, bufferedSize),
		bufferedSize: bufferedSize,
	}, nil
}

// append appends the given entry to the end of a file via the file descriptor it has.
func (w fileWAL) append(op walOperation, rows []Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch op {
	case operationInsert:
		for _, row := range rows {
			// Write the operation type
			if err := w.w.WriteByte(byte(op)); err != nil {
				return err
			}
			name := marshalMetricName(row.Metric, row.Labels)
			// Write the length of the metric name
			lBuf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(lBuf, uint64(len(name)))
			if _, err := w.w.Write(lBuf[:n]); err != nil {
				return err
			}
			// Write the metric name
			if _, err := w.w.WriteString(name); err != nil {
				return err
			}
			// Write the timestamp
			tsBuf := make([]byte, binary.MaxVarintLen64)
			n = binary.PutVarint(tsBuf, row.DataPoint.Timestamp)
			if _, err := w.w.Write(tsBuf[:n]); err != nil {
				return err
			}
			// Write the value
			vBuf := make([]byte, binary.MaxVarintLen64)
			n = binary.PutUvarint(vBuf, math.Float64bits(row.DataPoint.Value))
			if _, err := w.w.Write(vBuf[:n]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown operation %v given", op)
	}
	if w.bufferedSize == 0 {
		return w.flush()
	}

	return nil
}

func (w fileWAL) truncate(id int64) error {
	// FIXME: Truncate the specified index.
	//   To do so, make file with the name minTimestamp for each partition.
	os.RemoveAll(w.filename) // FIXME: remove
	return nil
}

// flush flushes all buffered entries to the underlying file.
func (w fileWAL) flush() error {
	return w.w.Flush()
}

func (w fileWAL) removeAll() error {
	return os.RemoveAll(w.filename)
}

type walRecord struct {
	op  walOperation
	row Row
}

type fileWALReader struct {
	file    *os.File
	r       *bufio.Reader
	current walRecord
	err     error
}

func newFileWalReader(filename string) (*fileWALReader, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %q: %w", filename, err)
	}

	return &fileWALReader{
		file: fd,
		r:    bufio.NewReader(fd),
	}, nil
}

func (f *fileWALReader) next() bool {
	op, err := f.r.ReadByte()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		f.err = err
		return false
	}
	switch walOperation(op) {
	case operationInsert:
		// Read the length of metric name.
		metricLen, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read the length of metric name: %w", err)
			return false
		}
		// Read the metric name.
		metric := make([]byte, int(metricLen))
		if _, err := io.ReadFull(f.r, metric); err != nil {
			f.err = fmt.Errorf("failed to read the metric name: %w", err)
			return false
		}
		// Read timestamp.
		ts, err := binary.ReadVarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read timestamp: %w", err)
			return false
		}
		// Read value.
		val, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read value: %w", err)
			return false
		}
		f.current = walRecord{
			op: walOperation(op),
			row: Row{
				Metric: string(metric),
				DataPoint: DataPoint{
					Timestamp: ts,
					Value:     math.Float64frombits(val),
				},
			},
		}
	default:
		f.err = fmt.Errorf("unknown operation %v found", op)
		return false
	}

	return true
}

// error gives back an error if it has been facing an error while reading.
func (f *fileWALReader) error() error {
	return f.err
}

func (f *fileWALReader) record() *walRecord {
	return &f.current
}

func (f *fileWALReader) close() error {
	return f.file.Close()
}
