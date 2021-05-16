package tstorage

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// PartitionList represents a linked list for partitions.
// Each partition is arranged in order order of newest to oldest.
// That is, the head node is always the newest, the tail node is the oldest.
// Only head partition should be writable.
//
// FYI: Partitions are frequently added/deleted, on the other hand,
// no need to take values by specifying indexes. That's why linked list is suitable.
type PartitionList interface {
	// Insert appends a new node to the head.
	Insert(partition partition)
	// Remove eliminates the given partition from the list.
	Remove(partition partition) error
	// Swap replaces the old partition with the new one.
	Swap(old, new partition) error
	// GetHead gives back the head node which is the newest one.
	GetHead() partition
	// Size returns the size of itself.
	Size() int
	// NewIterator gives back the iterator object fot this list.
	// If you need to inspect all nodes within the list, use this one.
	NewIterator() partitionIterator
}

// Iterator represents an iterator for partition list. The basic usage is:
/*
  for iterator.Next() {
    partition, err := iterator.Value()
    // Do something with partition
  }
*/
type partitionIterator interface {
	// Next positions the iterator at the next node in the list.
	// It will be positioned at the head on the first call.
	// The return value will be true if a value can be read from the list.
	Next() bool
	// Value gives back the current partition in the iterator.
	Value() (partition, error)

	currentNode() *partitionNode
}

type partitionList struct {
	size int64
	head *partitionNode
	tail *partitionNode
	mu   sync.RWMutex
}

func NewPartitionList() PartitionList {
	return &partitionList{}
}

func (p *partitionList) GetHead() partition {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.head.value()
}

func (p *partitionList) Insert(partition partition) {
	node := &partitionNode{
		val: partition,
	}
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	if head != nil {
		node.next = head
	}

	p.setHead(node)
	atomic.AddInt64(&p.size, 1)
}

func (p *partitionList) Remove(target partition) error {
	if p.Size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode
	iterator := p.NewIterator()
	for iterator.Next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), target) {
			prev = current
			continue
		}

		// Remove the current node.

		iterator.Next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// removing the head node
			p.setHead(next)
		case next == nil:
			// removing the tail node
			prev.setNext(nil)
			p.setTail(prev)
		default:
			// removing the middle node
			prev.setNext(next)
		}
		atomic.AddInt64(&p.size, -1)
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func (p *partitionList) Swap(old, new partition) error {
	if p.Size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode
	iterator := p.NewIterator()
	for iterator.Next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), old) {
			prev = current
			continue
		}

		// Swap the current node.

		newNode := &partitionNode{
			val:  new,
			next: current.next,
		}
		iterator.Next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// swapping the head node
			p.setHead(newNode)
		case next == nil:
			// swapping the tail node
			prev.setNext(newNode)
			p.setTail(newNode)
		default:
			// swapping the middle node
			prev.setNext(newNode)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func samePartitions(x, y partition) bool {
	// TODO: Use ULID for identifier of partition
	return x.MinTimestamp() == y.MinTimestamp()
}

func (p *partitionList) Size() int {
	return int(atomic.LoadInt64(&p.size))
}

func (p *partitionList) NewIterator() partitionIterator {
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	// Put a dummy node so that it positions the head on the first Next() call.
	dummy := &partitionNode{
		next: head,
	}
	return &partitionIteratorImpl{
		current: dummy,
	}
}

func (p *partitionList) setHead(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = node
}

func (p *partitionList) setTail(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tail = node
}

// partitionNode wraps a partition to hold the pointer to the next one.
type partitionNode struct {
	// val is immutable
	val  partition
	next *partitionNode
	mu   sync.RWMutex
}

// value gives back the actual partition of the node.
func (p *partitionNode) value() partition {
	return p.val
}

func (p *partitionNode) setNext(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = node
}

func (p *partitionNode) getNext() *partitionNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.next
}

type partitionIteratorImpl struct {
	current *partitionNode
}

func (i *partitionIteratorImpl) Next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *partitionIteratorImpl) Value() (partition, error) {
	if i.current == nil {
		return nil, fmt.Errorf("partition not found")
	}
	return i.current.value(), nil
}

func (i *partitionIteratorImpl) currentNode() *partitionNode {
	return i.current
}
