package tstorage

import "testing"

func BenchmarkStorage_InsertRows(b *testing.B) {
	storage, err := NewStorage()
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

func BenchmarkStorage_InsertRows_out_of_order(b *testing.B) {
	storage, err := NewStorage()
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}
