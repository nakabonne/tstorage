package tstorage

import (
	"fmt"
	"sort"
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
	if wal == nil {
		wal = &nopWAL{}
	}
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
	default:
		d = partitionDuration.Nanoseconds()
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
	m.wal.append(walEntry{
		operation: operationInsert,
		rows:      rows,
	})

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
		return t.UnixNano()
	}
}

func (m *memoryPartition) selectDataPoints(metric string, labels []Label, start, end int64) []*DataPoint {
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
			name:             name,
			points:           make([]*DataPoint, 0, 1000),
			outOfOrderPoints: make([]*DataPoint, 0),
		}
		m.metrics.Store(name, value)
	}
	return value.(*metric)
}

func (m *memoryPartition) selectAll() []Row {
	rows := make([]Row, 0, m.size())
	/*
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
	*/
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
	name         string
	size         int64
	minTimestamp int64
	maxTimestamp int64
	// points must kept in order
	points []*DataPoint
	// TODO: Merge out-of-order points when flushing
	outOfOrderPoints []*DataPoint
	mu               sync.RWMutex
}

func (m *metric) insertPoint(point *DataPoint) {
	size := atomic.LoadInt64(&m.size)
	// TODO: Consider to stop using mutex every time.
	//   Instead, fix the capacity of points slice, kind of like:
	/*
		m.points := make([]*DataPoint, 1000)
		for i := 0; i < 1000; i++ {
			m.points[i] = point
		}
	*/
	m.mu.Lock()
	defer m.mu.Unlock()

	// First insertion
	if size == 0 {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.minTimestamp, point.Timestamp)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}
	// Insert point in order
	if m.points[size-1].Timestamp < point.Timestamp {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}

	m.outOfOrderPoints = append(m.outOfOrderPoints, point)
}

// selectPoints returns a new slice by re-slicing with [startIdx:endIdx].
func (m *metric) selectPoints(start, end int64) []*DataPoint {
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)

	var startIdx, endIdx int
	m.mu.RLock()
	defer m.mu.RUnlock()
	if start <= minTimestamp {
		startIdx = 0
	} else {
		// Use binary search because m.points are in-order.
		startIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= start
		})
	}

	if end >= maxTimestamp {
		endIdx = int(size)
	} else {
		// Use binary search because m.points are in-order.
		endIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp < end
		}) + 1
	}
	return m.points[startIdx:endIdx]
}
