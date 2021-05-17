// Package tstorage provides goroutine safe capabilities of insertion into and retrieval
// from the time-series storage.
package tstorage

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nakabonne/tstorage/internal/cgroup"
	"github.com/nakabonne/tstorage/internal/timerpool"
)

var (
	// Limit the concurrency for data ingestion to GOMAXPROCS, since this operation
	// is CPU bound, so there is no sense in running more than GOMAXPROCS concurrent
	// goroutines on data ingestion path.
	defaultWorkersLimit = cgroup.AvailableCPUs()

	partitionDirRegex = regexp.MustCompile(`^p-.+`)
)

const (
	defaultPartitionDuration = 1 * time.Hour
	defaultWriteTimeout      = 30 * time.Second
)

// Storage provides goroutine safe capabilities of insertion into and retrieval from the time-series storage.
type Storage interface {
	Reader
	Writer
	// FlushRows persists all in-memory partitions ready to persisted.
	// FIXME: Maybe it should be done within this package
	FlushRows() error
}

// Reader provides reading access to time series data.
type Reader interface {
	// SelectRows gives back an iterator object to traverse data points within the given start-end range.
	// Keep in mind that start is inclusive, end is exclusive, and both must be Unix timestamp.
	// Typically the given iterator can be used to iterate over the data points, like:
	/*
		iterator, _, _ := storage.SelectRows(labels, 1600000, 1600001)
		for iterator.next() {
			fmt.Printf("value: %v\n", iterator.value())
		}
	*/
	SelectRows(metric string, labels []Label, start, end int64) (iterator DataPointIterator, size int, err error)
}

// Writer provides writing access to time series data.
type Writer interface {
	// InsertRows ingests the given rows to the time-series storage.
	InsertRows(rows []Row) error
	// Wait waits until all tasks got done.
	Wait()
}

// Row includs a data point along with properties to identify a kind of metrics.
type Row struct {
	// The unique name of metric.
	// This field must be set.
	Metric string
	// An optional key-value properties to further detailed identification.
	Labels []Label
	// This field must be set.
	DataPoint
}

// DataPoint represents a data point, the smallest unit of time series data.
type DataPoint struct {
	// The actual value. This field must be set.
	Value float64
	// Unix timestamp.
	// The current Unix time in nanoseconds will be populated if zero given.
	Timestamp int64
}

// Option is an optional setting for NewStorage.
type Option func(*storage)

// WithDataPath specifies the path to directory that stores time-series data.
// Use this to make time-series data persistent on disk.
// Defaults to empty string which means no data will get persisted.
func WithDataPath(dataPath string) Option {
	return func(s *storage) {
		s.dataPath = dataPath
	}
}

// WithPartitionDuration specifies the timestamp range of partitions,
//
// A partition is a chunk of time-series data with the timestamp range.
// It acts as a fully independent database containing all data
// points for its time range.
// Defaults to 1h
func WithPartitionDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.partitionDuration = duration
	}
}

// WithWriteTimeout specifies the timeout to wait when workers are busy.
//
// The store limits the number of concurrent goroutines to prevent from out of memory
// errors and CPU trashing even if too many goroutines attempt to write.
// Defaults to 30m.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}

// NewStorage gives back a new storage, which stores time-series data in the process memory by default.
//
// Give the WithDataPath option for running as a on-disk storage. Specify a directory with data already exists,
// then it will be read as the initial data.
func NewStorage(opts ...Option) (Storage, error) {
	s := &storage{
		partitionList:  newPartitionList(),
		workersLimitCh: make(chan struct{}, defaultWorkersLimit),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.partitionDuration <= 0 {
		s.partitionDuration = defaultPartitionDuration
	}
	if s.writeTimeout <= 0 {
		s.writeTimeout = defaultWriteTimeout
	}

	if s.inMemoryMode() {
		s.partitionList.insert(newMemoryPartition(nil, s.partitionDuration))
		return s, nil
	}

	s.wal = newFileWal(filepath.Join(s.dataPath, "wal"))
	if err := os.MkdirAll(s.dataPath, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make data directory %s: %w", s.dataPath, err)
	}
	files, err := ioutil.ReadDir(s.dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data directory: %w", err)
	}
	if len(files) == 0 {
		s.partitionList.insert(newMemoryPartition(s.wal, s.partitionDuration))
		return s, nil
	}

	// Read existent partitions from the disk.
	isPartitionDir := func(f fs.FileInfo) bool {
		return f.IsDir() && partitionDirRegex.MatchString(f.Name())
	}
	partitions := make([]partition, 0, len(files))
	for _, f := range files {
		if !isPartitionDir(f) {
			continue
		}
		path := filepath.Join(s.dataPath, f.Name())
		part, err := openDiskPartition(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open disk partition for %s: %w", path, err)
		}
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].minTimestamp() < partitions[j].minTimestamp()
	})
	for _, p := range partitions {
		s.partitionList.insert(p)
	}
	s.partitionList.insert(newMemoryPartition(s.wal, s.partitionDuration))

	return s, nil
}

type storage struct {
	partitionList partitionList

	wal               wal
	partitionDuration time.Duration
	dataPath          string
	writeTimeout      time.Duration

	workersLimitCh chan struct{}
	// wg must be incremented to guarantee all writes are done gracefully.
	wg sync.WaitGroup
}

func (s *storage) InsertRows(rows []Row) error {
	s.wg.Add(1)
	defer s.wg.Done()

	insert := func() error {
		defer func() { <-s.workersLimitCh }()
		p := s.getPartition()
		if err := p.insertRows(rows); err != nil {
			return fmt.Errorf("failed to insert rows: %w", err)
		}
		return nil
	}

	// Limit the number of concurrent goroutines to prevent from out of memory
	// errors and CPU trashing even if too many goroutines attempt to write.
	select {
	case s.workersLimitCh <- struct{}{}:
		return insert()
	default:
	}

	// Seems like all workers are busy; wait for up to writeTimeout

	t := timerpool.Get(s.writeTimeout)
	select {
	case s.workersLimitCh <- struct{}{}:
		timerpool.Put(t)
		return insert()
	case <-t.C:
		timerpool.Put(t)
		return fmt.Errorf("failed to write a data point in %s, since it is overloaded with %d concurrent writers",
			s.writeTimeout, defaultWorkersLimit)
	}
}

// getPartition returns a writable partition. If none, it creates a new one.
func (s *storage) getPartition() partition {
	head := s.partitionList.getHead()
	if !head.readOnly() {
		return head
	}

	// All partitions seems to be unavailable so add a new partition to the list.

	p := newMemoryPartition(s.wal, s.partitionDuration)
	s.partitionList.insert(p)
	return p
}

func (s *storage) SelectRows(metric string, labels []Label, start, end int64) (DataPointIterator, int, error) {
	if metric == "" {
		return nil, 0, fmt.Errorf("metric must be set")
	}
	if start >= end {
		return nil, 0, fmt.Errorf("thg given start is greater than end")
	}
	pointLists := make([]dataPointList, 0)

	// Iterate over all partitions from the newest one.
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return nil, 0, fmt.Errorf("unexpected empty partition found")
		}
		if part.maxTimestamp() < start {
			// No need to keep going anymore
			break
		}
		if part.minTimestamp() > end {
			continue
		}
		list := part.selectRows(metric, labels, start, end)
		// in order to keep the order in ascending.
		pointLists = append([]dataPointList{list}, pointLists...)
	}
	mergedList, err := mergeDataPointLists(pointLists...)
	if err != nil {
		return nil, 0, err
	}
	return mergedList.newIterator(), mergedList.size(), nil
}

func (s *storage) FlushRows() error {
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected empty partition found")
		}
		if p, ok := part.(inMemoryPartition); !ok || !p.ReadyToBePersisted() {
			continue
		}

		if s.inMemoryMode() {
			if err := s.partitionList.remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}

		// Start swapping in-memory partition for disk one.
		// The disk partition will place at where in-memory one existed.

		rows := make([]Row, 0, part.size())
		rows = append(rows, part.selectAll()...)
		// TODO: Use https://github.com/oklog/ulid instead of uuid
		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%s", uuid.New()))
		newPart, err := newDiskPartition(dir, rows, part.minTimestamp(), part.maxTimestamp())
		if err != nil {
			return fmt.Errorf("failed to generate disk partition for %s: %w", dir, err)
		}
		if err := s.partitionList.swap(part, newPart); err != nil {
			return fmt.Errorf("failed to swap partitions: %w", err)
		}
	}
	return nil
}

func (s *storage) Wait() {
	s.wg.Wait()
	// TODO: Prevent from new goroutines calling InsertRows(), for graceful shutdown.
	// FIXME: Flush data points within the all memory partition into the backend.
}

func (s *storage) inMemoryMode() bool {
	return s.dataPath == ""
}
