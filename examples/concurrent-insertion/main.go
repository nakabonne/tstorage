package main

import (
	"log"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, err := tstorage.NewStorage(
		tstorage.WithPartitionDuration(5 * time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := int64(1600000); i < 1610000; i++ {
		wg.Add(1)
		go func(timestamp int64) {
			if err := storage.InsertRows([]tstorage.Row{
				{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: timestamp}},
			}); err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	iterator, _, err := storage.SelectRows("metric1", nil, 1600000, 1610000)
	if err != nil {
		log.Fatal(err)
	}
	for iterator.Next() {
		log.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
}
