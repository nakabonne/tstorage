package tstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStorage_SelectDataPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	for i := 1; i < 100000; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_, _ = storage.SelectDataPoints("metric1", nil, 1, 100000)
	}
}

func BenchmarkStorage_InsertRows(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}
