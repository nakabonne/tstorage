package tstorage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkStorage_InsertRows(b *testing.B) {
	storage, err := NewStorage(
		WithPartitionDuration(1000000 * time.Hour),
	)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

func BenchmarkStorage_InsertRowsSlice(b *testing.B) {
	withNewHeadPartition := func(s *storage) {
		s.newHeadPartition = func(wal wal, d time.Duration) partition {
			m := &memoryPartition{
				wal:               wal,
				partitionDuration: d.Milliseconds(),
			}
			m.metrics.Store("metric1", &metric{
				name:   "metric1",
				points: newDataPointSlice(),
			})
			return m
		}
	}
	storage, err := NewStorage(
		WithPartitionDuration(1000000*time.Hour),
		withNewHeadPartition,
	)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

// dataPointSlice is a dataPointList implementation using slice instead of linked-list.
// This is for comparing the performance of slice and linked-list
type dataPointSlice struct {
	lastIndex int64
	points    []*DataPoint
	mu        sync.RWMutex
}

func newDataPointSlice() *dataPointSlice {
	return &dataPointSlice{
		lastIndex: -1,
		points:    []*DataPoint{},
	}
}

func (d *dataPointSlice) insert(point *DataPoint) {
	d.mu.Lock()
	defer d.mu.Unlock()
	defer atomic.AddInt64(&d.lastIndex, 1)

	// First insertion
	if d.lastIndex < 0 {
		d.points = append(d.points, point)
		return
	}
	// Append into the last
	if d.points[d.lastIndex].Timestamp < point.Timestamp {
		d.points = append(d.points, point)
		return
	}

	// Apparently the given data point has to be inserted in the middle.

	var i int
	for i = int(d.lastIndex); d.points[i].Timestamp < point.Timestamp; i-- {
	}
	// Start insertion into the certain point.
	//
	// 1, Resize: say new point is 2, and current slice is [1,3], then [1,3] => [1,3,2]
	d.points = append(d.points, point)
	// 2, Shift points one by one: e.g. [1,3,2] => [1,3,3]
	copy(d.points[i+1:], d.points[i:])
	// 3, Insert into the certain place.
	d.points[i] = point
}

func (d *dataPointSlice) size() int {
	return int(atomic.LoadInt64(&d.lastIndex) + 1)
}

func (d *dataPointSlice) newIterator() DataPointIterator {
	panic("implement me")
}
