package tstorage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_gzipCompressor_write_read(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "tstorage-gzip-test")
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	f, err := os.Create(filepath.Join(tmpDir, "data"))
	require.NoError(t, err)
	defer func() {
		err := f.Close()
		require.NoError(t, err)
	}()

	// Start writing data points after compressing
	writer := newGzipCompressor(f)
	input := []*DataPoint{
		{Timestamp: 1, Value: 0.1},
		{Timestamp: 2, Value: 0.1},
		{Timestamp: 3, Value: 0.1},
	}
	err = writer.write(input)
	require.NoError(t, err)
	err = writer.close()
	require.NoError(t, err)

	// Start reading data points after decompressing
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	reader, err := newGzipDecompressor(f)
	require.NoError(t, err)
	output := make([]*DataPoint, 0, len(input))
	for i := 0; i < len(input); i++ {
		p := &DataPoint{}
		err := reader.read(p)
		require.NoError(t, err)
		output = append(output, p)
	}
	err = reader.close()
	require.NoError(t, err)

	want := []*DataPoint{
		{Timestamp: 1, Value: 0.1},
		{Timestamp: 2, Value: 0.1},
		{Timestamp: 3, Value: 0.1},
	}
	assert.Equal(t, want, output)
}
