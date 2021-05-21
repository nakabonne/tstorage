package tstorage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// memoryPartition implements a partition to store on the process memory.
type memoryPartition struct {
	// A hash map from metric-name to metric.
	metrics sync.Map
	// The number of data points
	numPoints int64
	// minT is immutable.
	minT int64
	maxT int64

	// Write ahead log.
	wal wal
	// The timestamp range of partitions after which they get persisted
	partitionDuration  int64
	timestampPrecision TimestampPrecision
	once               sync.Once
}

func newMemoryPartition(wal wal, partitionDuration time.Duration, precision TimestampPrecision) partition {
	var d int64
	switch precision {
	case Nanoseconds:
		d = partitionDuration.Nanoseconds()
	case Microseconds:
		d = partitionDuration.Microseconds()
	case Milliseconds:
		d = partitionDuration.Milliseconds()
	case Seconds:
		d = int64(partitionDuration.Seconds())
	}
	return &memoryPartition{
		partitionDuration:  d,
		wal:                wal,
		timestampPrecision: precision,
	}
}

// insertRows inserts the given rows to partition.
func (m *memoryPartition) insertRows(rows []Row) ([]Row, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows given")
	}
	if m.wal != nil {
		m.wal.append(walEntry{
			operation: operationInsert,
			rows:      rows,
		})
	}

	// Set min timestamp at only first.
	m.once.Do(func() {
		min := rows[0].Timestamp
		for i := range rows {
			row := rows[i]
			if row.Timestamp < min {
				min = row.Timestamp
			}
		}
		atomic.StoreInt64(&m.minT, min)
	})

	outdatedRows := make([]Row, 0)
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for i := range rows {
		row := rows[i]
		if row.Timestamp < m.minTimestamp() {
			outdatedRows = append(outdatedRows, row)
			continue
		}
		if row.Timestamp == 0 {
			row.Timestamp = toUnix(time.Now(), m.timestampPrecision)
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

	// Make max timestamp up-to-date.
	if atomic.LoadInt64(&m.maxT) < maxTimestamp {
		atomic.SwapInt64(&m.maxT, maxTimestamp)
	}

	return outdatedRows, nil
}

func toUnix(t time.Time, precision TimestampPrecision) int64 {
	switch precision {
	case Nanoseconds:
		return t.UnixNano()
	case Microseconds:
		return t.UnixNano() / 1e3
	case Milliseconds:
		return t.UnixNano() / 1e6
	case Seconds:
		return t.Unix()
	default:
		return 0
	}
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

func (m *memoryPartition) minTimestamp() int64 {
	return atomic.LoadInt64(&m.minT)
}

func (m *memoryPartition) maxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxT)
}

func (m *memoryPartition) size() int {
	return int(atomic.LoadInt64(&m.numPoints))
}

func (m *memoryPartition) active() bool {
	return m.maxTimestamp()-m.minTimestamp() < m.partitionDuration
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
	// FIXME: Consider using binary search
	//   using slice may be better.
	//   Also, think about how to mutex
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
