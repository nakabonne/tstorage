package tstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	ErrNoDataPoints = errors.New("no data points found")

	// Limit the concurrency for data ingestion to GOMAXPROCS, since this operation
	// is CPU bound, so there is no sense in running more than GOMAXPROCS concurrent
	// goroutines on data ingestion path.
	defaultWorkersLimit = cgroup.AvailableCPUs()

	partitionDirRegex = regexp.MustCompile(`^p-.+`)
)

// TimestampPrecision represents precision of timestamps. See WithTimestampPrecision
type TimestampPrecision string

const (
	Nanoseconds  TimestampPrecision = "ns"
	Microseconds TimestampPrecision = "us"
	Milliseconds TimestampPrecision = "ms"
	Seconds      TimestampPrecision = "s"

	defaultPartitionDuration     = 1 * time.Hour
	defaultTimestampPrecision    = Nanoseconds
	defaultWriteTimeout          = 30 * time.Second
	defaultWritablePartitionsNum = 2
)

// Storage provides goroutine safe capabilities of insertion into and retrieval from the time-series storage.
type Storage interface {
	Reader
	// InsertRows ingests the given rows to the time-series storage.
	// If a timestamp isn't specified, it uses the machine's local timestamp in UTC.
	InsertRows(rows []Row) error
	// Close flushes all data points within the memory partitions into the backend.
	Close()
}

// Reader provides reading access to time series data.
type Reader interface {
	// SelectDataPoints gives back a list of data points  within the given start-end range.
	// Keep in mind that start is inclusive, end is exclusive, and both must be Unix timestamp.
	SelectDataPoints(metric string, labels []Label, start, end int64) (points []*DataPoint, err error)
}

// Row includes a data point along with properties to identify a kind of metrics.
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
	Timestamp int64
}

// Option is an optional setting for NewStorage.
type Option func(*storage)

// WithDataPath specifies the path to directory that stores time-series data.
// Use this to make time-series data persistent on disk.
//
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
//
// Defaults to 1h
func WithPartitionDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.partitionDuration = duration
	}
}

// WithTimestampPrecision specifies the precision of timestamps to be used by all operations.
//
// Defaults to Nanoseconds
func WithTimestampPrecision(precision TimestampPrecision) Option {
	return func(s *storage) {
		s.timestampPrecision = precision
	}
}

// WithWriteTimeout specifies the timeout to wait when workers are busy.
//
// The storage limits the number of concurrent goroutines to prevent from out of memory
// errors and CPU trashing even if too many goroutines attempt to write.
//
// Defaults to 30m.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}

// WithLogger specifies the logger to emit verbose output.
//
// Defaults to a logger implementation that does nothing.
func WithLogger(logger Logger) Option {
	return func(s *storage) {
		s.logger = logger
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
		// TODO: Make gzip compressor/decompressor changeable
		compressorFactory:   newGzipCompressor,
		decompressorFactory: newGzipDecompressor,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.partitionDuration <= 0 {
		s.partitionDuration = defaultPartitionDuration
	}
	if s.timestampPrecision == "" {
		s.timestampPrecision = defaultTimestampPrecision
	}
	if s.writeTimeout <= 0 {
		s.writeTimeout = defaultWriteTimeout
	}
	if s.logger == nil {
		s.logger = &nopLogger{}
	}
	if s.newHeadPartition == nil {
		s.newHeadPartition = newMemoryPartition
	}

	if s.inMemoryMode() {
		s.partitionList.insert(s.newHeadPartition(nil, s.partitionDuration, s.timestampPrecision))
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
		s.partitionList.insert(s.newHeadPartition(s.wal, s.partitionDuration, s.timestampPrecision))
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
		part, err := openDiskPartition(path, s.decompressorFactory)
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
	s.partitionList.insert(s.newHeadPartition(s.wal, s.partitionDuration, s.timestampPrecision))

	return s, nil
}

type storage struct {
	partitionList partitionList

	wal                 wal
	partitionDuration   time.Duration
	timestampPrecision  TimestampPrecision
	dataPath            string
	writeTimeout        time.Duration
	newHeadPartition    func(wal, time.Duration, TimestampPrecision) partition
	compressorFactory   func(w io.WriteSeeker) compressor
	decompressorFactory func(r io.Reader) (decompressor, error)

	logger         Logger
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
		outdatedRows, err := p.insertRows(rows)
		if err != nil {
			return fmt.Errorf("failed to insert rows: %w", err)
		}
		// TODO: Try to insert outdated rows to head's next partition
		_ = outdatedRows
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
	if head.active() {
		return head
	}

	// All partitions seems to be inactive so add a new partition to the list.

	p := s.newHeadPartition(s.wal, s.partitionDuration, s.timestampPrecision)
	s.partitionList.insert(p)
	go func() {
		if err := s.flushPartitions(); err != nil {
			s.logger.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()
	return p
}

func (s *storage) SelectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if metric == "" {
		return nil, fmt.Errorf("metric must be set")
	}
	if start >= end {
		return nil, fmt.Errorf("thg given start is greater than end")
	}
	points := make([]*DataPoint, 0)

	// Iterate over all partitions from the newest one.
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return nil, fmt.Errorf("unexpected empty partition found")
		}
		if part.maxTimestamp() < start {
			// No need to keep going anymore
			break
		}
		if part.minTimestamp() > end {
			continue
		}
		ps, err := part.selectDataPoints(metric, labels, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to select data points: %w", err)
		}
		// in order to keep the order in ascending.
		points = append(ps, points...)
	}
	if len(points) == 0 {
		return nil, ErrNoDataPoints
	}
	return points, nil
}

func (s *storage) Close() {
	s.wg.Wait()
	// TODO: Prevent from new goroutines calling InsertRows(), for graceful shutdown.
	// FIXME: Flush data points within the all memory partition into the backend.
}

// flushPartitions persists all in-memory partitions ready to persisted.
// For the in-memory mode, just removes it from the partition list.
func (s *storage) flushPartitions() error {
	// Keep the first two partitions as is even if they are inactive,
	// to accept out-of-order data points.
	i := 0
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		if i < defaultWritablePartitionsNum {
			i++
			continue
		}
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected empty partition found")
		}
		memPart, ok := part.(*memoryPartition)
		if !ok {
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

		// TODO: Use https://github.com/oklog/ulid instead of uuid
		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%s", uuid.New()))
		if err := flush(dir, memPart, s.compressorFactory); err != nil {
			return fmt.Errorf("failed to compact memory partition into %s: %w", dir, err)
		}
		newPart, err := openDiskPartition(dir, s.decompressorFactory)
		if err != nil {
			return fmt.Errorf("failed to generate disk partition for %s: %w", dir, err)
		}
		if err := s.partitionList.swap(part, newPart); err != nil {
			return fmt.Errorf("failed to swap partitions: %w", err)
		}
	}
	return nil
}

// flush compacts the data points in the given partition and flushes them to the given directory.
func flush(dirPath string, m *memoryPartition, compressorFactory func(seeker io.WriteSeeker) compressor) error {
	if dirPath == "" {
		return fmt.Errorf("dir path is required")
	}

	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}

	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", dirPath, err)
	}
	defer f.Close()
	compactor := compressorFactory(f)

	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*memoryMetric)
		if !ok {
			return false
		}
		// TODO: Merge out-of-order data points
		points := make([]*DataPoint, 0, len(mt.points)+len(mt.outOfOrderPoints))
		for _, p := range mt.points {
			points = append(points, p)
		}
		// Compress data points for each metric.
		if err := compactor.write(points); err != nil {
			return false
		}
		// FIXME: Write offset of metric start

		return true
	})
	if err := compactor.close(); err != nil {
		return err
	}

	b, err := json.Marshal(&meta{
		MinTimestamp:  m.minTimestamp(),
		MaxTimestamp:  m.maxTimestamp(),
		NumDatapoints: m.size(),
	})
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}
	return nil
}

func (s *storage) inMemoryMode() bool {
	return s.dataPath == ""
}
