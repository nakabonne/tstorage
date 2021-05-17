package tstorage

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const chunksDirName = "chunks"

// See newDiskPartition for details.
type diskPartition struct {
	dirPath string

	// The number of data points
	size int
	minT int64
	maxT int64
}

// newDiskPartition generates a disk partition from the given data.
// If any data exist under the given dirPath, it overrides data with the given initial data.
// It's typically used for making a brand new partition.
//
// A disk partition acts as a partition that uses local disk as a storage.
// Once initializing a disk partition, it is permanently immutable.
func newDiskPartition(dirPath string, rows []Row, minTimestamp, maxTimestamp int64) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("rows are required")
	}

	chunksDir := filepath.Join(dirPath, chunksDirName)
	if err := os.MkdirAll(chunksDir, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}

	// TODO: Divide chunks for each constant bytes
	dataPath := filepath.Join(chunksDir, "1")
	if err := os.WriteFile(dataPath, rowsToBytes(rows), fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to write data points to %s: %w", dataPath, err)
	}
	numDatapoints := len(rows)
	m := &meta{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		NumDatapoints: numDatapoints,
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}

	return &diskPartition{
		dirPath: dirPath,
		size:    numDatapoints,
		minT:    minTimestamp,
		maxT:    maxTimestamp,
	}, nil
}

// openDiskPartition generates a disk partition from the existent files.
// If the given dir doesn't exist, use newDiskPartition instead.
func openDiskPartition(dirPath string) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	f, err := os.Open(filepath.Join(dirPath, metaFileName))
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer f.Close()

	m := &meta{}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath: dirPath,
		minT:    m.MinTimestamp,
		maxT:    m.MaxTimestamp,
		size:    m.NumDatapoints,
	}, nil
}

func rowsToBytes(rows []Row) []byte {
	// FIXME: Compact rows
	return []byte("not implemented yet")
}

func (d *diskPartition) insertRows(_ []Row) error {
	return fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) selectRows(metric string, labels []Label, start, end int64) dataPointList {
	// TODO: Implement selectRows from disk partition
	fmt.Println("select rows for disk partition isn't implemented yet")
	return newDataPointList(nil, nil, 0)
}

func (d *diskPartition) selectAll() []Row {
	// TODO: Implement selectAll for disk partition
	fmt.Println("select all for disk partition isn't implemented yet")
	return []Row{}
}

func (d *diskPartition) MinTimestamp() int64 {
	return d.minT
}

func (d *diskPartition) MaxTimestamp() int64 {
	return d.maxT
}

func (d *diskPartition) Size() int {
	return d.size
}

// Disk partition is immutable.
func (d *diskPartition) ReadOnly() bool {
	return true
}

const metaFileName = "meta.json"

// meta is a mapper for a meta file, which is put for each partition.
type meta struct {
	MinTimestamp  int64 `json:"minTimestamp"`
	MaxTimestamp  int64 `json:"maxTimestamp"`
	NumDatapoints int   `json:"numDatapoints"`
}
