package tstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_diskWAL_append_read(t *testing.T) {
	var (
		op   = operationInsert
		rows = []Row{
			{Metric: "metric-1", DataPoint: DataPoint{Value: 0.1, Timestamp: 1600000000}},
			{Metric: "metric-2", DataPoint: DataPoint{Value: 0.2, Timestamp: 1600000001}},
			{Metric: "metric-1", DataPoint: DataPoint{Value: 0.1, Timestamp: 1600000001}},
			{Metric: "metric-2", DataPoint: DataPoint{Value: 0.2, Timestamp: 1600000003}},
		}
	)
	// Append rows into wal
	tmpDir, err := os.MkdirTemp("", "tstorage-test")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, err)
	path := filepath.Join(tmpDir, "wal")

	wal, err := newDiskWAL(path, 4096)
	require.NoError(t, err)

	// Append into two segments
	err = wal.append(op, rows[:2])
	require.NoError(t, err)

	err = wal.punctuate()
	require.NoError(t, err)

	err = wal.append(op, rows[2:])
	require.NoError(t, err)

	err = wal.flush()
	require.NoError(t, err)

	// Recover rows.
	reader, err := newDiskWALReader(path)
	require.NoError(t, err)
	err = reader.readAll()
	require.NoError(t, err)
	got := reader.rowsToInsert
	assert.Equal(t, rows, got)
}
