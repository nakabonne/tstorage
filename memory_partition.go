package tstorage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var _ MemoryPartition = &memoryPartition{}

// See NewMemoryPartition for details.
type memoryPartition struct {
	// A hash map from metric-name to metric.
	metrics sync.Map
	// Write ahead log.
	wal WAL
	// The number of data points
	size int64
	// The timestamp range of partitions after which they get persisted
	partitionDuration int64

	minTimestamp int64
	maxTimestamp int64
}

// NewMemoryPartition generates a partition to store on the process memory.
func NewMemoryPartition(wal WAL, partitionDuration time.Duration) Partition {
	return &memoryPartition{
		partitionDuration: partitionDuration.Milliseconds(),
		wal:               wal,
	}
}

// InsertRows inserts the given rows to partition.
func (m *memoryPartition) InsertRows(rows []Row) error {
	if len(rows) == 0 {
		return fmt.Errorf("no row was given")
	}
	if m.ReadOnly() {
		return fmt.Errorf("read only partition")
	}
	if m.wal != nil {
		m.wal.Append(Entry{
			Operation: OperationInsert,
			Rows:      rows,
		})
	}

	minTimestamp := rows[0].Timestamp
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for _, row := range rows {
		if row.Timestamp < minTimestamp {
			minTimestamp = row.Timestamp
		}
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}
		mt := m.getMetric(row.MetricName)
		mt.insertPoint(&row.DataPoint)
		rowsNum++
	}
	atomic.AddInt64(&m.size, rowsNum)

	// Make min/max timestamps up-to-date.
	if min := atomic.LoadInt64(&m.minTimestamp); min == 0 || min > minTimestamp {
		atomic.SwapInt64(&m.minTimestamp, minTimestamp)
	}
	if atomic.LoadInt64(&m.maxTimestamp) < maxTimestamp {
		atomic.SwapInt64(&m.maxTimestamp, maxTimestamp)
	}

	return nil
}

// SelectRows gives back the certain data points within the given range.
func (m *memoryPartition) SelectRows(metricName string, start, end int64) []DataPoint {
	mt := m.getMetric(metricName)
	return mt.selectPoints(start, end)
}

// getMetric gives back the reference to the metrics list whose name is the given one.
// If none, it creates a new one.
func (m *memoryPartition) getMetric(name string) *metric {
	value, ok := m.metrics.Load(name)
	if !ok {
		value = &metric{
			name:      name,
			points:    make([]DataPoint, 0),
			lastIndex: -1,
		}
		m.metrics.Store(name, value)
	}
	return value.(*metric)
}

func (m *memoryPartition) SelectAll() []Row {
	rows := make([]Row, 0, m.Size())
	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*metric)
		if !ok {
			return false
		}
		k, ok := key.(string)
		if !ok {
			return false
		}
		for _, point := range mt.points {
			rows = append(rows, Row{
				MetricName: k,
				DataPoint: DataPoint{
					Timestamp: point.Timestamp,
					Value:     point.Value,
				},
			})
		}
		return true
	})
	return rows
}

func (m *memoryPartition) ReadOnly() bool {
	return m.MaxTimestamp()-m.MinTimestamp() > m.partitionDuration
}

func (m *memoryPartition) MinTimestamp() int64 {
	return atomic.LoadInt64(&m.minTimestamp)
}

func (m *memoryPartition) MaxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxTimestamp)
}

func (m *memoryPartition) Size() int {
	return int(atomic.LoadInt64(&m.size))
}

func (m *memoryPartition) ReadyToBePersisted() bool {
	return m.ReadOnly()
}

// metric has a list of data points that belong to the metric
type metric struct {
	name string
	// FIXME: Use linked list instead of slice
	points []DataPoint
	mu     sync.RWMutex

	// Use this to arrange points in ascending order.
	lastIndex int
}

func (m *metric) insertPoint(point *DataPoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() { m.lastIndex++ }() // TODO: Carefully check how it goes

	if m.lastIndex < 0 {
		m.points = append(m.points, *point)
		return
	}
	if m.points[m.lastIndex].Timestamp < point.Timestamp {
		m.points = append(m.points, *point)
		return
	}

	// Apparently the given data point has to be inserted in the middle.

	var i int
	for i = m.lastIndex; m.points[i].Timestamp < point.Timestamp; i-- {
	}
	// Start insertion into the certain point.
	//
	// 1, Resize: say new point is 2, and current slice is [1,3], then [1,3] => [1,3,2]
	m.points = append(m.points, *point)
	// 2, Shift points one by one: e.g. [1,3,2] => [1,3,3]
	copy(m.points[i+1:], m.points[i:])
	// 3, Insert into the certain place.
	m.points[i] = *point
}

func (m *metric) selectPoints(start, end int64) []DataPoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	points := make([]DataPoint, 0)
	for _, p := range m.points {
		if p.Timestamp > end {
			break
		}
		if p.Timestamp > start {
			points = append(points, p)
		}
	}
	return points
}
