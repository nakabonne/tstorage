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
	labels := []tstorage.Label{
		{
			Name:  "__name__",
			Value: "metric1",
		},
	}
	err = storage.InsertRows([]tstorage.Row{
		{
			Labels: labels,
			DataPoint: tstorage.DataPoint{
				Timestamp: 1600000,
				Value:     0.1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	iterator, _, err := storage.SelectRows(labels, 1600000, 1600001)
	if err != nil {
		log.Fatal(err)
	}
	for iterator.Next() {
		log.Printf("value: %v\n", iterator.Value())
	}
}
