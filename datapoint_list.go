package tstorage

import (
	"sync"
	"sync/atomic"
)

// dataPointList represents a linked list for data points.
// Each dataPoint is arranged in order order of oldest to newest.
// That is, the head node is always the oldest, the tail node is the newest.
//
// FYI: Data points are frequently added/deleted, on the other hand,
// no need to take values by specifying indexes. That's why linked list is suitable.
type dataPointList interface {
	// insert puts a data point to a the appropriate point.
	insert(point *DataPoint)
	// size returns the number of data points.
	size() int
	// newIterator gives back the iterator object fot this list.
	// If you need to inspect all nodes within the list, use this one.
	newIterator() DataPointIterator
}

// DataPointIterator represents an iterator for data point list. The basic usage is:
/*
  for iterator.Next() {
    point, err := iterator.Value()
    // Do something withdataPoint
  }
*/
type DataPointIterator interface {
	// Next positions the iterator at the next node in the list.
	// It will be positioned at the head on the first call.
	// The return value will be true if a value can be read from the list.
	Next() bool
	// Value gives back the current dataPoint in the iterator.
	// Don't call for nil node.
	Value() *DataPoint

	// node gives back the current node itself.
	node() *dataPointNode
}

type dataPointListImpl struct {
	numPoints int64
	head      *dataPointNode
	tail      *dataPointNode
	mu        sync.RWMutex
}

// newDataPointList optionally accepts the initial head and tail nodes.
func newDataPointList(head, tail *dataPointNode, size int64) dataPointList {
	return &dataPointListImpl{
		head:      head,
		tail:      tail,
		numPoints: size,
	}
}

func (l *dataPointListImpl) insert(point *DataPoint) {
	newNode := &dataPointNode{
		val: point,
	}
	tail := l.getTail()
	if tail.value().Timestamp < point.Timestamp {
		// Append to the tail
		newNode.setPrev(tail)
		tail.setNext(newNode)
		l.setTail(newNode)
		return
	}

	// FIXME: Insert out-of-order data point to appropriate place.

	atomic.AddInt64(&l.numPoints, 1)
}

func (l *dataPointListImpl) size() int {
	return int(atomic.LoadInt64(&l.numPoints))
}

func (l *dataPointListImpl) newIterator() DataPointIterator {
	l.mu.RLock()
	head := l.head
	l.mu.RUnlock()
	// Put a dummy node so that it positions the head on the first Next() call.
	dummy := &dataPointNode{
		next: head,
	}
	return &dataPointIterator{
		current: dummy,
	}
}

func (l *dataPointListImpl) setHead(node *dataPointNode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.head = node
}

func (l *dataPointListImpl) setTail(node *dataPointNode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tail = node
}

func (l *dataPointListImpl) getTail() *dataPointNode {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tail
}

// dataPointNode wraps a dataPoint to hold the pointer to the next/prev one.
type dataPointNode struct {
	// val is immutable
	val  *DataPoint
	next *dataPointNode
	prev *dataPointNode
	mu   sync.RWMutex
}

// value gives back the actual dataPoint of the node.
func (n *dataPointNode) value() *DataPoint {
	return n.val
}

func (n *dataPointNode) setNext(node *dataPointNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.next = node
}

func (n *dataPointNode) getNext() *dataPointNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.next
}

func (n *dataPointNode) setPrev(node *dataPointNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.prev = node
}

func (n *dataPointNode) getPrev() *dataPointNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.prev
}

type dataPointIterator struct {
	current *dataPointNode
}

func (i *dataPointIterator) Next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *dataPointIterator) Value() *DataPoint {
	if i.current == nil {
		return nil
	}
	return i.current.value()
}

func (i *dataPointIterator) node() *dataPointNode {
	return i.current
}
