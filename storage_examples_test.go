package tstorage_test

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
)

func ExampleNewStorage_withDataPath() {
	// It will make time-series data persistent under "./data".
	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath("./data"),
	)
	if err != nil {
		panic(err)
	}
	storage.Close()
}

func ExampleNewStorage_withPartitionDuration() {
	storage, err := tstorage.NewStorage(
		tstorage.WithPartitionDuration(30*time.Minute),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer storage.Close()
}

func ExampleStorage_InsertRows() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()
	err = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
	})
	if err != nil {
		panic(err)
	}
	points, err := storage.Select("metric1", nil, 1600000000, 1600000001)
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	// timestamp: 1600000000, value: 0.1
}

// simulates writing and reading in concurrent.
func ExampleStorage_InsertRows_Select_concurrent() {
	storage, err := tstorage.NewStorage(
		tstorage.WithPartitionDuration(5*time.Hour),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup

	// Start write workers that insert 10000 times in concurrent, as fast as possible.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1600000000); i < 1600010000; i++ {
			wg.Add(1)
			go func(timestamp int64) {
				defer wg.Done()
				if err := storage.InsertRows([]tstorage.Row{
					{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp}},
				}); err != nil {
					panic(err)
				}
			}(i)
		}
	}()

	// Start read workers that read 100 times in concurrent, as fast as possible.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				points, err := storage.Select("metric1", nil, 1600000000, 1600010000)
				if errors.Is(err, tstorage.ErrNoDataPoints) {
					return
				}
				if err != nil {
					panic(err)
				}
				for _, p := range points {
					_ = p.Timestamp
					_ = p.Value
				}
			}()
		}
	}()
	wg.Wait()
}

func ExampleStorage_Select_from_memory() {
	tmpDir, err := os.MkdirTemp("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(2*time.Hour),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	// Ingest data points of metric1
	for timestamp := int64(1600000000); timestamp < 1600000050; timestamp++ {
		err := storage.InsertRows([]tstorage.Row{
			{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: 0.1}},
		})
		if err != nil {
			panic(err)
		}
	}
	// Ingest data points of metric2
	for timestamp := int64(1600000050); timestamp < 1600000100; timestamp++ {
		err := storage.InsertRows([]tstorage.Row{
			{Metric: "metric2", DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: 0.2}},
		})
		if err != nil {
			panic(err)
		}
	}

	points, err := storage.Select("metric1", nil, 1600000000, 1600000050)
	if errors.Is(err, tstorage.ErrNoDataPoints) {
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("Data points of metric1:")
	for _, p := range points {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}

	points2, err := storage.Select("metric2", nil, 1600000050, 1600000100)
	if errors.Is(err, tstorage.ErrNoDataPoints) {
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("Data points of metric2:")
	for _, p := range points2 {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	//Data points of metric1:
	//Timestamp: 1600000000, Value: 0.1
	//Timestamp: 1600000001, Value: 0.1
	//Timestamp: 1600000002, Value: 0.1
	//Timestamp: 1600000003, Value: 0.1
	//Timestamp: 1600000004, Value: 0.1
	//Timestamp: 1600000005, Value: 0.1
	//Timestamp: 1600000006, Value: 0.1
	//Timestamp: 1600000007, Value: 0.1
	//Timestamp: 1600000008, Value: 0.1
	//Timestamp: 1600000009, Value: 0.1
	//Timestamp: 1600000010, Value: 0.1
	//Timestamp: 1600000011, Value: 0.1
	//Timestamp: 1600000012, Value: 0.1
	//Timestamp: 1600000013, Value: 0.1
	//Timestamp: 1600000014, Value: 0.1
	//Timestamp: 1600000015, Value: 0.1
	//Timestamp: 1600000016, Value: 0.1
	//Timestamp: 1600000017, Value: 0.1
	//Timestamp: 1600000018, Value: 0.1
	//Timestamp: 1600000019, Value: 0.1
	//Timestamp: 1600000020, Value: 0.1
	//Timestamp: 1600000021, Value: 0.1
	//Timestamp: 1600000022, Value: 0.1
	//Timestamp: 1600000023, Value: 0.1
	//Timestamp: 1600000024, Value: 0.1
	//Timestamp: 1600000025, Value: 0.1
	//Timestamp: 1600000026, Value: 0.1
	//Timestamp: 1600000027, Value: 0.1
	//Timestamp: 1600000028, Value: 0.1
	//Timestamp: 1600000029, Value: 0.1
	//Timestamp: 1600000030, Value: 0.1
	//Timestamp: 1600000031, Value: 0.1
	//Timestamp: 1600000032, Value: 0.1
	//Timestamp: 1600000033, Value: 0.1
	//Timestamp: 1600000034, Value: 0.1
	//Timestamp: 1600000035, Value: 0.1
	//Timestamp: 1600000036, Value: 0.1
	//Timestamp: 1600000037, Value: 0.1
	//Timestamp: 1600000038, Value: 0.1
	//Timestamp: 1600000039, Value: 0.1
	//Timestamp: 1600000040, Value: 0.1
	//Timestamp: 1600000041, Value: 0.1
	//Timestamp: 1600000042, Value: 0.1
	//Timestamp: 1600000043, Value: 0.1
	//Timestamp: 1600000044, Value: 0.1
	//Timestamp: 1600000045, Value: 0.1
	//Timestamp: 1600000046, Value: 0.1
	//Timestamp: 1600000047, Value: 0.1
	//Timestamp: 1600000048, Value: 0.1
	//Timestamp: 1600000049, Value: 0.1
	//Data points of metric2:
	//Timestamp: 1600000050, Value: 0.2
	//Timestamp: 1600000051, Value: 0.2
	//Timestamp: 1600000052, Value: 0.2
	//Timestamp: 1600000053, Value: 0.2
	//Timestamp: 1600000054, Value: 0.2
	//Timestamp: 1600000055, Value: 0.2
	//Timestamp: 1600000056, Value: 0.2
	//Timestamp: 1600000057, Value: 0.2
	//Timestamp: 1600000058, Value: 0.2
	//Timestamp: 1600000059, Value: 0.2
	//Timestamp: 1600000060, Value: 0.2
	//Timestamp: 1600000061, Value: 0.2
	//Timestamp: 1600000062, Value: 0.2
	//Timestamp: 1600000063, Value: 0.2
	//Timestamp: 1600000064, Value: 0.2
	//Timestamp: 1600000065, Value: 0.2
	//Timestamp: 1600000066, Value: 0.2
	//Timestamp: 1600000067, Value: 0.2
	//Timestamp: 1600000068, Value: 0.2
	//Timestamp: 1600000069, Value: 0.2
	//Timestamp: 1600000070, Value: 0.2
	//Timestamp: 1600000071, Value: 0.2
	//Timestamp: 1600000072, Value: 0.2
	//Timestamp: 1600000073, Value: 0.2
	//Timestamp: 1600000074, Value: 0.2
	//Timestamp: 1600000075, Value: 0.2
	//Timestamp: 1600000076, Value: 0.2
	//Timestamp: 1600000077, Value: 0.2
	//Timestamp: 1600000078, Value: 0.2
	//Timestamp: 1600000079, Value: 0.2
	//Timestamp: 1600000080, Value: 0.2
	//Timestamp: 1600000081, Value: 0.2
	//Timestamp: 1600000082, Value: 0.2
	//Timestamp: 1600000083, Value: 0.2
	//Timestamp: 1600000084, Value: 0.2
	//Timestamp: 1600000085, Value: 0.2
	//Timestamp: 1600000086, Value: 0.2
	//Timestamp: 1600000087, Value: 0.2
	//Timestamp: 1600000088, Value: 0.2
	//Timestamp: 1600000089, Value: 0.2
	//Timestamp: 1600000090, Value: 0.2
	//Timestamp: 1600000091, Value: 0.2
	//Timestamp: 1600000092, Value: 0.2
	//Timestamp: 1600000093, Value: 0.2
	//Timestamp: 1600000094, Value: 0.2
	//Timestamp: 1600000095, Value: 0.2
	//Timestamp: 1600000096, Value: 0.2
	//Timestamp: 1600000097, Value: 0.2
	//Timestamp: 1600000098, Value: 0.2
	//Timestamp: 1600000099, Value: 0.2
}

// simulates writing and reading on disk.
func ExampleStorage_Select_from_disk() {
	tmpDir, err := os.MkdirTemp("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(100*time.Second),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

	// Ingest data points
	for timestamp := int64(1600000000); timestamp < 1600000050; timestamp++ {
		err := storage.InsertRows([]tstorage.Row{
			{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: 0.1}},
		})
		if err != nil {
			panic(err)
		}
		err = storage.InsertRows([]tstorage.Row{
			{Metric: "metric2", DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: 0.2}},
		})
		if err != nil {
			panic(err)
		}
	}
	// Flush all data points
	if err := storage.Close(); err != nil {
		panic(err)
	}

	// Re-open storage from the persisted data
	storage, err = tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(10*time.Second),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	points, err := storage.Select("metric1", nil, 1600000000, 1600000050)
	if errors.Is(err, tstorage.ErrNoDataPoints) {
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("Data points of metric1:")
	for _, p := range points {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}

	points2, err := storage.Select("metric2", nil, 1600000000, 1600000050)
	if errors.Is(err, tstorage.ErrNoDataPoints) {
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("Data points of metric2:")
	for _, p := range points2 {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	//Data points of metric1:
	//Timestamp: 1600000000, Value: 0.1
	//Timestamp: 1600000001, Value: 0.1
	//Timestamp: 1600000002, Value: 0.1
	//Timestamp: 1600000003, Value: 0.1
	//Timestamp: 1600000004, Value: 0.1
	//Timestamp: 1600000005, Value: 0.1
	//Timestamp: 1600000006, Value: 0.1
	//Timestamp: 1600000007, Value: 0.1
	//Timestamp: 1600000008, Value: 0.1
	//Timestamp: 1600000009, Value: 0.1
	//Timestamp: 1600000010, Value: 0.1
	//Timestamp: 1600000011, Value: 0.1
	//Timestamp: 1600000012, Value: 0.1
	//Timestamp: 1600000013, Value: 0.1
	//Timestamp: 1600000014, Value: 0.1
	//Timestamp: 1600000015, Value: 0.1
	//Timestamp: 1600000016, Value: 0.1
	//Timestamp: 1600000017, Value: 0.1
	//Timestamp: 1600000018, Value: 0.1
	//Timestamp: 1600000019, Value: 0.1
	//Timestamp: 1600000020, Value: 0.1
	//Timestamp: 1600000021, Value: 0.1
	//Timestamp: 1600000022, Value: 0.1
	//Timestamp: 1600000023, Value: 0.1
	//Timestamp: 1600000024, Value: 0.1
	//Timestamp: 1600000025, Value: 0.1
	//Timestamp: 1600000026, Value: 0.1
	//Timestamp: 1600000027, Value: 0.1
	//Timestamp: 1600000028, Value: 0.1
	//Timestamp: 1600000029, Value: 0.1
	//Timestamp: 1600000030, Value: 0.1
	//Timestamp: 1600000031, Value: 0.1
	//Timestamp: 1600000032, Value: 0.1
	//Timestamp: 1600000033, Value: 0.1
	//Timestamp: 1600000034, Value: 0.1
	//Timestamp: 1600000035, Value: 0.1
	//Timestamp: 1600000036, Value: 0.1
	//Timestamp: 1600000037, Value: 0.1
	//Timestamp: 1600000038, Value: 0.1
	//Timestamp: 1600000039, Value: 0.1
	//Timestamp: 1600000040, Value: 0.1
	//Timestamp: 1600000041, Value: 0.1
	//Timestamp: 1600000042, Value: 0.1
	//Timestamp: 1600000043, Value: 0.1
	//Timestamp: 1600000044, Value: 0.1
	//Timestamp: 1600000045, Value: 0.1
	//Timestamp: 1600000046, Value: 0.1
	//Timestamp: 1600000047, Value: 0.1
	//Timestamp: 1600000048, Value: 0.1
	//Timestamp: 1600000049, Value: 0.1
	//Data points of metric2:
	//Timestamp: 1600000000, Value: 0.2
	//Timestamp: 1600000001, Value: 0.2
	//Timestamp: 1600000002, Value: 0.2
	//Timestamp: 1600000003, Value: 0.2
	//Timestamp: 1600000004, Value: 0.2
	//Timestamp: 1600000005, Value: 0.2
	//Timestamp: 1600000006, Value: 0.2
	//Timestamp: 1600000007, Value: 0.2
	//Timestamp: 1600000008, Value: 0.2
	//Timestamp: 1600000009, Value: 0.2
	//Timestamp: 1600000010, Value: 0.2
	//Timestamp: 1600000011, Value: 0.2
	//Timestamp: 1600000012, Value: 0.2
	//Timestamp: 1600000013, Value: 0.2
	//Timestamp: 1600000014, Value: 0.2
	//Timestamp: 1600000015, Value: 0.2
	//Timestamp: 1600000016, Value: 0.2
	//Timestamp: 1600000017, Value: 0.2
	//Timestamp: 1600000018, Value: 0.2
	//Timestamp: 1600000019, Value: 0.2
	//Timestamp: 1600000020, Value: 0.2
	//Timestamp: 1600000021, Value: 0.2
	//Timestamp: 1600000022, Value: 0.2
	//Timestamp: 1600000023, Value: 0.2
	//Timestamp: 1600000024, Value: 0.2
	//Timestamp: 1600000025, Value: 0.2
	//Timestamp: 1600000026, Value: 0.2
	//Timestamp: 1600000027, Value: 0.2
	//Timestamp: 1600000028, Value: 0.2
	//Timestamp: 1600000029, Value: 0.2
	//Timestamp: 1600000030, Value: 0.2
	//Timestamp: 1600000031, Value: 0.2
	//Timestamp: 1600000032, Value: 0.2
	//Timestamp: 1600000033, Value: 0.2
	//Timestamp: 1600000034, Value: 0.2
	//Timestamp: 1600000035, Value: 0.2
	//Timestamp: 1600000036, Value: 0.2
	//Timestamp: 1600000037, Value: 0.2
	//Timestamp: 1600000038, Value: 0.2
	//Timestamp: 1600000039, Value: 0.2
	//Timestamp: 1600000040, Value: 0.2
	//Timestamp: 1600000041, Value: 0.2
	//Timestamp: 1600000042, Value: 0.2
	//Timestamp: 1600000043, Value: 0.2
	//Timestamp: 1600000044, Value: 0.2
	//Timestamp: 1600000045, Value: 0.2
	//Timestamp: 1600000046, Value: 0.2
	//Timestamp: 1600000047, Value: 0.2
	//Timestamp: 1600000048, Value: 0.2
	//Timestamp: 1600000049, Value: 0.2
}

// Out of order data points that are not yet flushed are in the buffer
// but do not appear in select.
func ExampleStorage_Select_from_memory_out_of_order() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()
	err = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000002, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000001, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000003, Value: 0.1}},
	})
	if err != nil {
		panic(err)
	}
	points, err := storage.Select("metric1", nil, 1600000000, 1600000003)
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}

	// Out-of-order data points are ignored because they will get merged when flushing.

	// Output:
	// Timestamp: 1600000000, Value: 0.1
	// Timestamp: 1600000002, Value: 0.1
	// Timestamp: 1600000003, Value: 0.1
}

// Out of order data points that are flushed should appear in select.
func ExampleStorage_Select_from_disk_out_of_order() {
	tmpDir, err := os.MkdirTemp("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(100*time.Second),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

	err = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000002, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000001, Value: 0.1}},
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000003, Value: 0.1}},
	})
	if err != nil {
		panic(err)
	}

	// Flush all data points
	if err := storage.Close(); err != nil {
		panic(err)
	}

	// Re-open storage from the persisted data
	storage, err = tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(100*time.Second),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	points, err := storage.Select("metric1", nil, 1600000000, 1600000004)
	if errors.Is(err, tstorage.ErrNoDataPoints) {
		return
	}
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	// timestamp: 1600000000, value: 0.1
	// timestamp: 1600000001, value: 0.1
	// timestamp: 1600000002, value: 0.1
	// timestamp: 1600000003, value: 0.1
}

// Simulates inserting an outdated row that forces inserting into a non-head partition.
func ExampleStorage_InsertRows_outdated() {
	tmpDir, err := os.MkdirTemp("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithPartitionDuration(3*time.Second),
	)
	if err != nil {
		panic(err)
	}

	// Force two partitions with timestamps: (min: 1, max: 3), (min: 4, max: 5)
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000001, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000003, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000004, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000005, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}

	// Insert a data point that doesn't belong to the head partition. This will be inserted
	// into the next partition out of order.
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000002, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}

	// Flush all data points
	if err := storage.Close(); err != nil {
		panic(err)
	}

	// Re-open storage from the persisted data
	storage, err = tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithPartitionDuration(3*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	points, err := storage.Select("metric1", nil, 1600000001, 1600000006)
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	// Timestamp: 1600000001, Value: 0.1
	// Timestamp: 1600000002, Value: 0.1
	// Timestamp: 1600000003, Value: 0.1
	// Timestamp: 1600000004, Value: 0.1
	// Timestamp: 1600000005, Value: 0.1
}

// Simulates inserting a row that's outside of the writable time window.
func ExampleStorage_InsertRows_expired() {
	tmpDir, err := os.MkdirTemp("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithPartitionDuration(3*time.Second),
	)
	if err != nil {
		panic(err)
	}

	// Force three partitions with timestamps: (min: 1, max: 3), (min: 4, max: 6), (min: 7, max: 8).
	// Inserting the third partition will force the first one to be flushed to disk and become unwritable.
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000001, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000003, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000004, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000005, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000006, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000007, Value: 0.1}, Metric: "metric1"},
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000008, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}

	// Try to insert a data point into an already flushed partition.
	err = storage.InsertRows([]tstorage.Row{
		{DataPoint: tstorage.DataPoint{Timestamp: 1600000002, Value: 0.1}, Metric: "metric1"},
	})
	if err != nil {
		panic(err)
	}

	// Flush all data points
	if err := storage.Close(); err != nil {
		panic(err)
	}

	// Re-open storage from the persisted data
	storage, err = tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithPartitionDuration(3*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()

	points, err := storage.Select("metric1", nil, 1600000001, 1600000009)
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("Timestamp: %v, Value: %v\n", p.Timestamp, p.Value)
	}

	// Missing data point at 1600000002 because it was dropped.

	// Output:
	// Timestamp: 1600000001, Value: 0.1
	// Timestamp: 1600000003, Value: 0.1
	// Timestamp: 1600000004, Value: 0.1
	// Timestamp: 1600000005, Value: 0.1
	// Timestamp: 1600000006, Value: 0.1
	// Timestamp: 1600000007, Value: 0.1
	// Timestamp: 1600000008, Value: 0.1
}

func ExampleStorage_InsertRows_concurrent() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer storage.Close()

	// First insert in order to ensure min timestamp
	if err := storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000}},
	}); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := int64(1600000001); i < 1600000100; i++ {
		wg.Add(1)
		go func(timestamp int64) {
			if err := storage.InsertRows([]tstorage.Row{
				{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp}},
			}); err != nil {
				panic(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	points, err := storage.Select("metric1", nil, 1600000000, 1600000100)
	if err != nil {
		panic(err)
	}
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
}
