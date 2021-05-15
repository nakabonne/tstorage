package tstorage

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
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

// Storage provides goroutine safe capabilities of insertion into and retrieval from partitions.
type Storage interface {
	Reader
	Writer
	// FlushRows persists all in-memory partitions ready to persisted.
	// TODO: Maybe it should be done within this package
	FlushRows() error
}

// Reader provides reading access to time series data.
type Reader interface {
	SelectRows(metricName string, start, end int64) []DataPoint
}

// Writer provides writing access to time series data.
type Writer interface {
	InsertRows(rows []Row) error
	// Wait waits until all tasks got done.
	Wait()
}

type Option func(*storage)

func WithDataPath(dataPath string) Option {
	return func(s *storage) {
		s.dataPath = dataPath
	}
}

func WithPartitionDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.partitionDuration = duration
	}
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}

// NewStorage gives back a new storage along with the initial partition.
// Give the WithDataPath option for running as a on-disk storage.
func NewStorage(opts ...Option) (Storage, error) {
	s := &storage{
		partitionList:  NewPartitionList(),
		workersLimitCh: make(chan struct{}, defaultWorkersLimit),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.partitionDuration <= 0 {
		s.partitionDuration = defaultPartitionDuration
	}
	if s.partitionDuration <= 0 {
		s.writeTimeout = defaultWriteTimeout
	}

	if s.inMemoryMode() {
		s.partitionList.Insert(NewMemoryPartition(nil, s.partitionDuration))
		return s, nil
	}

	s.wal = NewFileWAL(filepath.Join(s.dataPath, "wal"))
	if err := os.MkdirAll(s.dataPath, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make data directory %s: %w", s.dataPath, err)
	}
	files, err := ioutil.ReadDir(s.dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data directory: %w", err)
	}
	if len(files) == 0 {
		s.partitionList.Insert(NewMemoryPartition(s.wal, s.partitionDuration))
		return s, nil
	}

	// Read existent partitions from the disk.
	isPartitionDir := func(f fs.FileInfo) bool {
		return f.IsDir() && partitionDirRegex.MatchString(f.Name())
	}
	partitions := make([]Partition, 0, len(files))
	for _, f := range files {
		if !isPartitionDir(f) {
			continue
		}
		path := filepath.Join(s.dataPath, f.Name())
		part, err := OpenDiskPartition(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open disk partition for %s: %w", path, err)
		}
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].MinTimestamp() < partitions[j].MinTimestamp()
	})
	for _, p := range partitions {
		s.partitionList.Insert(p)
	}
	s.partitionList.Insert(NewMemoryPartition(s.wal, s.partitionDuration))

	return s, nil
}

type storage struct {
	partitionList PartitionList

	wal               WAL
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
		if err := p.InsertRows(rows); err != nil {
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
func (s *storage) getPartition() Partition {
	head := s.partitionList.GetHead()
	if !head.ReadOnly() {
		return head
	}

	// All partitions seems to be unavailable so add a new partition to the list.

	p := NewMemoryPartition(s.wal, s.partitionDuration)
	s.partitionList.Insert(p)
	return p
}

func (s *storage) SelectRows(metricName string, start, end int64) []DataPoint {
	res := make([]DataPoint, 0)

	// Iterate over all partitions from the newest one.
	iterator := s.partitionList.NewIterator()
	for iterator.Next() {
		part, err := iterator.Value()
		if err != nil {
			// TODO: Replace logger
			log.Printf("invalid partition found: %v\n", err)
			continue
		}
		if part.MaxTimestamp() < start {
			// No need to keep going anymore
			break
		}
		if part.MinTimestamp() > end {
			continue
		}
		points := part.SelectRows(metricName, start, end)
		// in order to keep the order in ascending.
		res = append(points, res...)
	}
	return res
}

func (s *storage) FlushRows() error {
	iterator := s.partitionList.NewIterator()
	for iterator.Next() {
		part, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("invalid partition found: %w", err)
		}
		if p, ok := part.(MemoryPartition); !ok || !p.ReadyToBePersisted() {
			continue
		}

		if s.inMemoryMode() {
			if err := s.partitionList.Remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}

		// Start swapping in-memory partition for disk one.
		// The disk partition will place at where in-memory one existed.

		rows := make([]Row, 0, part.Size())
		rows = append(rows, part.SelectAll()...)
		// TODO: Use https://github.com/oklog/ulid instead of uuid
		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%s", uuid.New()))
		newPart, err := NewDiskPartition(dir, rows, part.MinTimestamp(), part.MaxTimestamp())
		if err != nil {
			return fmt.Errorf("failed to generate disk partition for %s: %w", dir, err)
		}
		if err := s.partitionList.Swap(part, newPart); err != nil {
			return fmt.Errorf("failed to swap partitions: %w", err)
		}
	}
	return nil
}

func (s *storage) Wait() {
	s.wg.Wait()
	// TODO: Prevent from new goroutines calling Write(), for graceful shutdown.
	// TODO: Flush data points within the all memory partition into the backend.
}

func (s *storage) inMemoryMode() bool {
	return s.dataPath == ""
}
