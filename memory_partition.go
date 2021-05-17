package tstorage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var _ inMemoryPartition = &memoryPartition{}

// memoryPartition implements a partition to store on the process memory.
type memoryPartition struct {
	// A hash map from metric-name to metric.
	metrics sync.Map
	// Write ahead log.
	wal wal
	// The number of data points
	numPoints int64
	// The timestamp range of partitions after which they get persisted
	partitionDuration int64

	minT int64
	maxT int64
}

func newMemoryPartition(wal wal, partitionDuration time.Duration) partition {
	return &memoryPartition{
		partitionDuration: partitionDuration.Milliseconds(),
		wal:               wal,
	}
}

// insertRows inserts the given rows to partition.
func (m *memoryPartition) insertRows(rows []Row) error {
	if len(rows) == 0 {
		return fmt.Errorf("no row was given")
	}
	if m.readOnly() {
		return fmt.Errorf("read only partition")
	}
	if m.wal != nil {
		m.wal.append(walEntry{
			operation: operationInsert,
			rows:      rows,
		})
	}

	minTimestamp := rows[0].Timestamp
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for i := range rows {
		row := rows[i]
		if row.Timestamp == 0 {
			row.Timestamp = time.Now().UnixNano()
		}
		if row.Timestamp < minTimestamp {
			minTimestamp = row.Timestamp
		}
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}
		name := marshalMetricName(row.Metric, row.Labels)
		mt := m.getMetric(name)
		mt.insertPoint(&row.DataPoint)
		rowsNum++
	}
	atomic.AddInt64(&m.numPoints, rowsNum)

	// Make min/max timestamps up-to-date.
	if min := atomic.LoadInt64(&m.minT); min == 0 || min > minTimestamp {
		atomic.SwapInt64(&m.minT, minTimestamp)
	}
	if atomic.LoadInt64(&m.maxT) < maxTimestamp {
		atomic.SwapInt64(&m.maxT, maxTimestamp)
	}

	return nil
}

// selectRows gives back the certain data points within the given range.
func (m *memoryPartition) selectRows(metric string, labels []Label, start, end int64) dataPointList {
	name := marshalMetricName(metric, labels)
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

func (m *memoryPartition) selectAll() []Row {
	rows := make([]Row, 0, m.size())
	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*metric)
		if !ok {
			return false
		}
		k, ok := key.(string)
		if !ok {
			return false
		}
		labels := unmarshalMetricName(k)
		iterator := mt.points.newIterator()
		for iterator.Next() {
			point := iterator.DataPoint()
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

func (m *memoryPartition) readOnly() bool {
	// FIXME: Align time unit between min/max timestamp and partitionDuration
	return m.maxTimestamp()-m.minTimestamp() > m.partitionDuration
}

func (m *memoryPartition) minTimestamp() int64 {
	return atomic.LoadInt64(&m.minT)
}

func (m *memoryPartition) maxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxT)
}

func (m *memoryPartition) size() int {
	return int(atomic.LoadInt64(&m.numPoints))
}

func (m *memoryPartition) ReadyToBePersisted() bool {
	return m.readOnly()
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

// selectPoints returns a new dataPointList. It just takes head and tail out and sets them to the new one.
func (m *metric) selectPoints(start, end int64) dataPointList {
	// Position the iterator at the node to be head.
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

	// Position the iterator at the node to be tail.
	var tail *dataPointNode
	var num int64 = 1
	prev := head
	for iterator.Next() {
		num++
		current := iterator.node()
		if current.value().Timestamp < end {
			prev = current
			continue
		}
		tail = prev
		break
	}
	return newDataPointList(head, tail, num)
}
