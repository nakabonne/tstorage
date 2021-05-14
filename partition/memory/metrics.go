package memory

import (
	"sync"

	"github.com/nakabonne/tstorage/partition"
)

// metric has a list of data points that belong to the metric
type metric struct {
	name string
	// FIXME: Use linked list instead of slice
	points []partition.DataPoint
	mu     sync.RWMutex

	// Use this to arrange points in ascending order.
	lastIndex int
}

func (m *metric) insertPoint(point *partition.DataPoint) {
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

func (m *metric) selectPoints(start, end int64) []partition.DataPoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	points := make([]partition.DataPoint, 0)
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
