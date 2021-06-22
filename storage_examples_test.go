package tstorage_test

import (
	"errors"
	"fmt"
	"io/ioutil"
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

// simulates writing and reading on disk.
func ExampleStorage_InsertRows_Select_on_disk() {
	tmpDir, err := ioutil.TempDir("", "tstorage-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(tmpDir),
		tstorage.WithPartitionDuration(10*time.Second),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

	for timestamp := int64(1600000000); timestamp < 1600000050; timestamp++ {
		err := storage.InsertRows([]tstorage.Row{
			{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp}},
		})
		if err != nil {
			panic(err)
		}
	}
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
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
	// Output:
	//timestamp: 1600000000, value: 0
	//timestamp: 1600000001, value: 0
	//timestamp: 1600000002, value: 0
	//timestamp: 1600000003, value: 0
	//timestamp: 1600000004, value: 0
	//timestamp: 1600000005, value: 0
	//timestamp: 1600000006, value: 0
	//timestamp: 1600000007, value: 0
	//timestamp: 1600000008, value: 0
	//timestamp: 1600000009, value: 0
	//timestamp: 1600000010, value: 0
	//timestamp: 1600000011, value: 0
	//timestamp: 1600000012, value: 0
	//timestamp: 1600000013, value: 0
	//timestamp: 1600000014, value: 0
	//timestamp: 1600000015, value: 0
	//timestamp: 1600000016, value: 0
	//timestamp: 1600000017, value: 0
	//timestamp: 1600000018, value: 0
	//timestamp: 1600000019, value: 0
	//timestamp: 1600000020, value: 0
	//timestamp: 1600000021, value: 0
	//timestamp: 1600000022, value: 0
	//timestamp: 1600000023, value: 0
	//timestamp: 1600000024, value: 0
	//timestamp: 1600000025, value: 0
	//timestamp: 1600000026, value: 0
	//timestamp: 1600000027, value: 0
	//timestamp: 1600000028, value: 0
	//timestamp: 1600000029, value: 0
	//timestamp: 1600000030, value: 0
	//timestamp: 1600000031, value: 0
	//timestamp: 1600000032, value: 0
	//timestamp: 1600000033, value: 0
	//timestamp: 1600000034, value: 0
	//timestamp: 1600000035, value: 0
	//timestamp: 1600000036, value: 0
	//timestamp: 1600000037, value: 0
	//timestamp: 1600000038, value: 0
	//timestamp: 1600000039, value: 0
	//timestamp: 1600000040, value: 0
	//timestamp: 1600000041, value: 0
	//timestamp: 1600000042, value: 0
	//timestamp: 1600000043, value: 0
	//timestamp: 1600000044, value: 0
	//timestamp: 1600000045, value: 0
	//timestamp: 1600000046, value: 0
	//timestamp: 1600000047, value: 0
	//timestamp: 1600000048, value: 0
	//timestamp: 1600000049, value: 0
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
