package tstorage_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
)

func ExampleStorage_InsertRows_simple() {
	storage, err := tstorage.NewStorage()
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000, Value: 0.1}},
	})
	if err != nil {
		panic(err)
	}
	iterator, size, err := storage.SelectRows("metric1", nil, 1600000, 1600001)
	if err != nil {
		panic(err)
	}
	fmt.Printf("size: %d\n", size)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	// size: 1
	// timestamp: 1600000, value: 0.1
}

// ExampleStorage_InsertRows_SelectRows_concurrent simulates writing and reading in concurrent.
func ExampleStorage_InsertRows_SelectRows_concurrent() {
	storage, err := tstorage.NewStorage(
		tstorage.WithPartitionDuration(5 * time.Hour),
	)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	// Start write workers that insert 10000 times in concurrent, as fast as possible.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1600000); i < 1610000; i++ {
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
				iterator, _, err := storage.SelectRows("metric1", nil, 1600000, 1610000)
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
	storage, err := tstorage.NewStorage()
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := int64(1600000); i < 1600100; i++ {
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

	iterator, size, err := storage.SelectRows("metric1", nil, 1600000, 1600100)
	if err != nil {
		panic(err)
	}
	fmt.Printf("size: %d\n", size)
	// FIXME: Look into why sometimes 1600000 or 1600001 are missing
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	//size: 100
	//timestamp: 1600000, value: 0
	//timestamp: 1600001, value: 0
	//timestamp: 1600002, value: 0
	//timestamp: 1600003, value: 0
	//timestamp: 1600004, value: 0
	//timestamp: 1600005, value: 0
	//timestamp: 1600006, value: 0
	//timestamp: 1600007, value: 0
	//timestamp: 1600008, value: 0
	//timestamp: 1600009, value: 0
	//timestamp: 1600010, value: 0
	//timestamp: 1600011, value: 0
	//timestamp: 1600012, value: 0
	//timestamp: 1600013, value: 0
	//timestamp: 1600014, value: 0
	//timestamp: 1600015, value: 0
	//timestamp: 1600016, value: 0
	//timestamp: 1600017, value: 0
	//timestamp: 1600018, value: 0
	//timestamp: 1600019, value: 0
	//timestamp: 1600020, value: 0
	//timestamp: 1600021, value: 0
	//timestamp: 1600022, value: 0
	//timestamp: 1600023, value: 0
	//timestamp: 1600024, value: 0
	//timestamp: 1600025, value: 0
	//timestamp: 1600026, value: 0
	//timestamp: 1600027, value: 0
	//timestamp: 1600028, value: 0
	//timestamp: 1600029, value: 0
	//timestamp: 1600030, value: 0
	//timestamp: 1600031, value: 0
	//timestamp: 1600032, value: 0
	//timestamp: 1600033, value: 0
	//timestamp: 1600034, value: 0
	//timestamp: 1600035, value: 0
	//timestamp: 1600036, value: 0
	//timestamp: 1600037, value: 0
	//timestamp: 1600038, value: 0
	//timestamp: 1600039, value: 0
	//timestamp: 1600040, value: 0
	//timestamp: 1600041, value: 0
	//timestamp: 1600042, value: 0
	//timestamp: 1600043, value: 0
	//timestamp: 1600044, value: 0
	//timestamp: 1600045, value: 0
	//timestamp: 1600046, value: 0
	//timestamp: 1600047, value: 0
	//timestamp: 1600048, value: 0
	//timestamp: 1600049, value: 0
	//timestamp: 1600050, value: 0
	//timestamp: 1600051, value: 0
	//timestamp: 1600052, value: 0
	//timestamp: 1600053, value: 0
	//timestamp: 1600054, value: 0
	//timestamp: 1600055, value: 0
	//timestamp: 1600056, value: 0
	//timestamp: 1600057, value: 0
	//timestamp: 1600058, value: 0
	//timestamp: 1600059, value: 0
	//timestamp: 1600060, value: 0
	//timestamp: 1600061, value: 0
	//timestamp: 1600062, value: 0
	//timestamp: 1600063, value: 0
	//timestamp: 1600064, value: 0
	//timestamp: 1600065, value: 0
	//timestamp: 1600066, value: 0
	//timestamp: 1600067, value: 0
	//timestamp: 1600068, value: 0
	//timestamp: 1600069, value: 0
	//timestamp: 1600070, value: 0
	//timestamp: 1600071, value: 0
	//timestamp: 1600072, value: 0
	//timestamp: 1600073, value: 0
	//timestamp: 1600074, value: 0
	//timestamp: 1600075, value: 0
	//timestamp: 1600076, value: 0
	//timestamp: 1600077, value: 0
	//timestamp: 1600078, value: 0
	//timestamp: 1600079, value: 0
	//timestamp: 1600080, value: 0
	//timestamp: 1600081, value: 0
	//timestamp: 1600082, value: 0
	//timestamp: 1600083, value: 0
	//timestamp: 1600084, value: 0
	//timestamp: 1600085, value: 0
	//timestamp: 1600086, value: 0
	//timestamp: 1600087, value: 0
	//timestamp: 1600088, value: 0
	//timestamp: 1600089, value: 0
	//timestamp: 1600090, value: 0
	//timestamp: 1600091, value: 0
	//timestamp: 1600092, value: 0
	//timestamp: 1600093, value: 0
	//timestamp: 1600094, value: 0
	//timestamp: 1600095, value: 0
	//timestamp: 1600096, value: 0
	//timestamp: 1600097, value: 0
	//timestamp: 1600098, value: 0
	//timestamp: 1600099, value: 0
}
