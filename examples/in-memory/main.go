package main

import (
	"log"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, err := tstorage.NewStorage()
	if err != nil {
		log.Fatal(err)
	}
	err = storage.InsertRows([]tstorage.Row{
		{
			Metric: "metric1",
			DataPoint: tstorage.DataPoint{
				Timestamp: 1600000,
				Value:     0.1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	iterator, _, err := storage.SelectRows("metric1", nil, 1600000, 1600001)
	if err != nil {
		log.Fatal(err)
	}
	for iterator.Next() {
		log.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
}
