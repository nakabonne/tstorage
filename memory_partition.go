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
	for i := range rows {
		row := rows[i]
		if row.Timestamp < minTimestamp {
			minTimestamp = row.Timestamp
		}
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}
		name := MarshalMetricName(row.Labels)
		mt := m.getMetric(name)
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
func (m *memoryPartition) SelectRows(labels []Label, start, end int64) dataPointList {
	name := MarshalMetricName(labels)
	mt := m.getMetric(name)
	return mt.selectPoints(start, end)
}

// getMetric gives back the reference to the metrics list whose name is the given one.
// If none, it creates a new one.
func (m *memoryPartition) getMetric(name string) *metric {
	value, ok := m.metrics.Load(name)
	if !ok {
		value = &metric{
			name:   name,
			points: newDataPointList(nil, nil, 0),
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
		labels := UnmarshalMetricName(k)
		iterator := mt.points.newIterator()
		for iterator.Next() {
			point := iterator.Value()
			rows = append(rows, Row{
				Labels: labels,
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
	name   string
	points dataPointList
	mu     sync.RWMutex
}

func (m *metric) insertPoint(point *DataPoint) {
	m.points.insert(point)
}

func (m *metric) selectPoints(start, end int64) dataPointList {
	// Just take the head and the tail.
	var head *dataPointNode
	iterator := m.points.newIterator()
	for iterator.Next() {
		current := iterator.node()
		if current.value().Timestamp < start {
			continue
		}
		head = current
		break
	}

	var tail *dataPointNode
	var num int64 = 1
	prev := head
	for iterator.Next() {
		num++
		current := iterator.node()
		if current.value().Timestamp > end {
			tail = prev
			break
		}
		prev = current
	}
	return newDataPointList(head, tail, num)
}
