package tstorage_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
)

func ExampleStorage_InsertRows_simple() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
	})
	if err != nil {
		panic(err)
	}
	iterator, size, err := storage.SelectRows("metric1", nil, 1600000000, 1600000001)
	if err != nil {
		panic(err)
	}
	fmt.Printf("size: %d\n", size)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	// size: 1
	// timestamp: 1600000000, value: 0.1
}

// ExampleStorage_InsertRows_SelectRows_concurrent simulates writing and reading in concurrent.
func ExampleStorage_InsertRows_SelectRows_concurrent() {
	storage, err := tstorage.NewStorage(
		tstorage.WithPartitionDuration(5*time.Hour),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

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
				iterator, _, err := storage.SelectRows("metric1", nil, 1600000000, 1600010000)
				if errors.Is(err, tstorage.ErrNoDataPoints) {
					return
				}
				if err != nil {
					panic(err)
				}
				for iterator.Next() {
					_ = iterator.DataPoint().Timestamp
					_ = iterator.DataPoint().Value
				}
			}()
		}
	}()
	wg.Wait()
}

func ExampleStorage_InsertRows_concurrent() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

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

	iterator, size, err := storage.SelectRows("metric1", nil, 1600000000, 1600000100)
	if err != nil {
		panic(err)
	}
	fmt.Printf("size: %d\n", size)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	//size: 100
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
	//timestamp: 1600000050, value: 0
	//timestamp: 1600000051, value: 0
	//timestamp: 1600000052, value: 0
	//timestamp: 1600000053, value: 0
	//timestamp: 1600000054, value: 0
	//timestamp: 1600000055, value: 0
	//timestamp: 1600000056, value: 0
	//timestamp: 1600000057, value: 0
	//timestamp: 1600000058, value: 0
	//timestamp: 1600000059, value: 0
	//timestamp: 1600000060, value: 0
	//timestamp: 1600000061, value: 0
	//timestamp: 1600000062, value: 0
	//timestamp: 1600000063, value: 0
	//timestamp: 1600000064, value: 0
	//timestamp: 1600000065, value: 0
	//timestamp: 1600000066, value: 0
	//timestamp: 1600000067, value: 0
	//timestamp: 1600000068, value: 0
	//timestamp: 1600000069, value: 0
	//timestamp: 1600000070, value: 0
	//timestamp: 1600000071, value: 0
	//timestamp: 1600000072, value: 0
	//timestamp: 1600000073, value: 0
	//timestamp: 1600000074, value: 0
	//timestamp: 1600000075, value: 0
	//timestamp: 1600000076, value: 0
	//timestamp: 1600000077, value: 0
	//timestamp: 1600000078, value: 0
	//timestamp: 1600000079, value: 0
	//timestamp: 1600000080, value: 0
	//timestamp: 1600000081, value: 0
	//timestamp: 1600000082, value: 0
	//timestamp: 1600000083, value: 0
	//timestamp: 1600000084, value: 0
	//timestamp: 1600000085, value: 0
	//timestamp: 1600000086, value: 0
	//timestamp: 1600000087, value: 0
	//timestamp: 1600000088, value: 0
	//timestamp: 1600000089, value: 0
	//timestamp: 1600000090, value: 0
	//timestamp: 1600000091, value: 0
	//timestamp: 1600000092, value: 0
	//timestamp: 1600000093, value: 0
	//timestamp: 1600000094, value: 0
	//timestamp: 1600000095, value: 0
	//timestamp: 1600000096, value: 0
	//timestamp: 1600000097, value: 0
	//timestamp: 1600000098, value: 0
	//timestamp: 1600000099, value: 0
}

func ExampleStorage_InsertRows_concurrent_out_of_order() {
	storage, err := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		panic(err)
	}

	// First insert in order to ensure min timestamp
	if err := storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000}},
	}); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	// Start insertion in descending order.
	for i := int64(1600000099); i > 1600000000; i-- {
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

	iterator, size, err := storage.SelectRows("metric1", nil, 1600000000, 1600000099)
	if err != nil {
		panic(err)
	}
	fmt.Printf("size: %d\n", size)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	//size: 100
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
	//timestamp: 1600000050, value: 0
	//timestamp: 1600000051, value: 0
	//timestamp: 1600000052, value: 0
	//timestamp: 1600000053, value: 0
	//timestamp: 1600000054, value: 0
	//timestamp: 1600000055, value: 0
	//timestamp: 1600000056, value: 0
	//timestamp: 1600000057, value: 0
	//timestamp: 1600000058, value: 0
	//timestamp: 1600000059, value: 0
	//timestamp: 1600000060, value: 0
	//timestamp: 1600000061, value: 0
	//timestamp: 1600000062, value: 0
	//timestamp: 1600000063, value: 0
	//timestamp: 1600000064, value: 0
	//timestamp: 1600000065, value: 0
	//timestamp: 1600000066, value: 0
	//timestamp: 1600000067, value: 0
	//timestamp: 1600000068, value: 0
	//timestamp: 1600000069, value: 0
	//timestamp: 1600000070, value: 0
	//timestamp: 1600000071, value: 0
	//timestamp: 1600000072, value: 0
	//timestamp: 1600000073, value: 0
	//timestamp: 1600000074, value: 0
	//timestamp: 1600000075, value: 0
	//timestamp: 1600000076, value: 0
	//timestamp: 1600000077, value: 0
	//timestamp: 1600000078, value: 0
	//timestamp: 1600000079, value: 0
	//timestamp: 1600000080, value: 0
	//timestamp: 1600000081, value: 0
	//timestamp: 1600000082, value: 0
	//timestamp: 1600000083, value: 0
	//timestamp: 1600000084, value: 0
	//timestamp: 1600000085, value: 0
	//timestamp: 1600000086, value: 0
	//timestamp: 1600000087, value: 0
	//timestamp: 1600000088, value: 0
	//timestamp: 1600000089, value: 0
	//timestamp: 1600000090, value: 0
	//timestamp: 1600000091, value: 0
	//timestamp: 1600000092, value: 0
	//timestamp: 1600000093, value: 0
	//timestamp: 1600000094, value: 0
	//timestamp: 1600000095, value: 0
	//timestamp: 1600000096, value: 0
	//timestamp: 1600000097, value: 0
	//timestamp: 1600000098, value: 0
	//timestamp: 1600000099, value: 0
}
