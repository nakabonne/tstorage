package tstorage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

// A disk partition acts as a partition that uses local disk as a storage.
// Once initializing a disk partition, it is permanently immutable; no need
// to lock at all.
type diskPartition struct {
	dirPath string
	// The number of data points
	numPoints int
	minT      int64
	maxT      int64
	f         *os.File
	// memory-mapped file backed by f
	mappedFile []byte
	// A hash map from metric name to diskMetric.
	metrics map[string]diskMetric

	decompressorFactory func(r io.Reader) (decompressor, error)
}

type diskMetric struct {
	name         string
	offset       int64
	numPoints    int64
	minTimestamp int64
	maxTimestamp int64
}

// meta is a mapper for a meta file, which is put for each partition.
type meta struct {
	MinTimestamp  int64 `json:"minTimestamp"`
	MaxTimestamp  int64 `json:"maxTimestamp"`
	NumDatapoints int   `json:"numDatapoints"`
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
	mapped, err := syscall.Mmap(
		int(f.Fd()),
		0,
		int(info.Size()),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// FIXME: Read metrics' offset

	// Read metadata
	m := &meta{}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath:             dirPath,
		minT:                m.MinTimestamp,
		maxT:                m.MaxTimestamp,
		numPoints:           m.NumDatapoints,
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
	mt, ok := d.metrics[name]
	if !ok {
		return nil, ErrNoDataPoints
	}
	r := bytes.NewReader(d.mappedFile)
	if _, err := r.Seek(mt.offset, 0); err != nil {
		return nil, err
	}

	decompressor, err := d.decompressorFactory(r)
	if err != nil {
		return nil, err
	}
	points := make([]*DataPoint, 0, mt.numPoints)
	for i := 0; i < d.numPoints; i++ {
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
	return d.minT
}

func (d *diskPartition) maxTimestamp() int64 {
	return d.maxT
}

func (d *diskPartition) size() int {
	return d.numPoints
}

// Disk partition is immutable.
func (d *diskPartition) active() bool {
	return false
}
