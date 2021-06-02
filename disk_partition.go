package tstorage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nakabonne/tstorage/internal/syscall"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

// A disk partition implements a partition that uses local disk as a storage.
// It mainly has two files, data file and meta file.
// The data file is memory-mapped and read only; no need to lock at all.
type diskPartition struct {
	dirPath string
	meta    meta
	// file descriptor of data file
	f *os.File
	// memory-mapped file backed by f
	mappedFile []byte

	decompressorFactory func(r io.Reader) (decompressor, error)
}

// meta is a mapper for a meta file, which is put for each partition.
type meta struct {
	MinTimestamp  int64                 `json:"minTimestamp"`
	MaxTimestamp  int64                 `json:"maxTimestamp"`
	NumDataPoints int                   `json:"numDataPoints"`
	Metrics       map[string]diskMetric `json:"metrics"`
}

// diskMetric holds meta data to access actual data from the memory-mapped file.
type diskMetric struct {
	Name          string `json:"name"`
	Offset        int64  `json:"offset"`
	MinTimestamp  int64  `json:"minTimestamp"`
	MaxTimestamp  int64  `json:"maxTimestamp"`
	NumDataPoints int64  `json:"numDataPoints"`
}

// openDiskPartition first maps the data file into memory with memory-mapping.
func openDiskPartition(dirPath string, decompressorFactory func(r io.Reader) (decompressor, error)) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	f, err := os.Open(filepath.Join(dirPath, metaFileName))
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file info: %w", err)
	}
	mapped, err := syscall.Mmap(int(f.Fd()), int(info.Size()))
	if err != nil {
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// Read metadata
	m := meta{}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath:             dirPath,
		meta:                m,
		f:                   f,
		mappedFile:          mapped,
		decompressorFactory: decompressorFactory,
	}, nil
}

func (d *diskPartition) insertRows(_ []Row) ([]Row, error) {
	return nil, fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	name := marshalMetricName(metric, labels)
	mt, ok := d.meta.Metrics[name]
	if !ok {
		return nil, ErrNoDataPoints
	}
	r := bytes.NewReader(d.mappedFile)
	if _, err := r.Seek(mt.Offset, 0); err != nil {
		return nil, err
	}

	decompressor, err := d.decompressorFactory(r)
	if err != nil {
		return nil, err
	}
	points := make([]*DataPoint, 0, mt.NumDataPoints)
	for i := 0; i < int(mt.NumDataPoints); i++ {
		point := &DataPoint{}
		if err := decompressor.read(point); err != nil {
			return nil, err
		}
		if point.Timestamp < start {
			continue
		}
		if point.Timestamp >= end {
			break
		}
		points = append(points, point)
	}
	if err := decompressor.close(); err != nil {
		return nil, err
	}
	return points, nil
}

func (d *diskPartition) minTimestamp() int64 {
	return d.meta.MinTimestamp
}

func (d *diskPartition) maxTimestamp() int64 {
	return d.meta.MaxTimestamp
}

func (d *diskPartition) size() int {
	return d.meta.NumDataPoints
}

// Disk partition is immutable.
func (d *diskPartition) active() bool {
	return false
}
