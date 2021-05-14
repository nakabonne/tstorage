package disk

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nakabonne/fiatdb/app/fiatd/storage/partition"
)

// See NewDiskPartition for details.
type diskPartition struct {
	dirPath string

	// The number of data points
	size         int
	minTimestamp int64
	maxTimestamp int64
}

const chunksDirName = "chunks"

// NewDiskPartition generates a disk partition from the given data.
// If any data exist under the given dirPath, it overrides data with the given initial data.
// It's typically used for making a brand new partition.
//
// A disk partition acts as a partition that uses local disk as a storage.
// Once initializing a disk partition, it is permanently immutable.
func NewDiskPartition(dirPath string, rows []partition.Row, minTimestamp, maxTimestamp int64) (partition.Partition, error) {
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
		dirPath:      dirPath,
		size:         numDatapoints,
		minTimestamp: minTimestamp,
		maxTimestamp: maxTimestamp,
	}, nil
}

// OpenDiskPartition generates a disk partition from the existent files.
// If the given dir doesn't exist, use NewDiskPartition instead.
func OpenDiskPartition(dirPath string) (partition.Partition, error) {
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
		dirPath:      dirPath,
		minTimestamp: m.MinTimestamp,
		maxTimestamp: m.MaxTimestamp,
		size:         m.NumDatapoints,
	}, nil
}

func rowsToBytes(rows []partition.Row) []byte {
	// FIXME: Compact rows
	return []byte("not implemented yet")
}

func (d *diskPartition) InsertRows(_ []partition.Row) error {
	return fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) SelectRows(metricName string, start, end int64) []partition.DataPoint {
	// FIXME: Implement SelectRows from disk partition
	fmt.Println("select rows for disk partition isn't implemented yet")
	return []partition.DataPoint{}
}

func (d *diskPartition) SelectAll() []partition.Row {
	// TODO: Implement SelectAll for disk partition
	fmt.Println("select all for disk partition isn't implemented yet")
	return []partition.Row{}
}

func (d *diskPartition) MinTimestamp() int64 {
	return d.minTimestamp
}

func (d *diskPartition) MaxTimestamp() int64 {
	return d.maxTimestamp
}

func (d *diskPartition) Size() int {
	return d.size
}

// Disk partition is immutable.
func (d *diskPartition) ReadOnly() bool {
	return true
}
