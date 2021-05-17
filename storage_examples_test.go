package tstorage

import "fmt"

func ExampleStorage_InsertRows() {
	storage, err := NewStorage()
	if err != nil {
		panic(err)
	}
	err = storage.InsertRows([]Row{
		{
			Metric:    "metric1",
			DataPoint: DataPoint{Timestamp: 1600000, Value: 0.1},
		},
	})
	if err != nil {
		panic(err)
	}
	iterator, size, err := storage.SelectRows("metric1", nil, 1600000, 1600001)
	if err != nil {
		panic(err)
	}
	fmt.Println("size:", size)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
	}
	// Output:
	// size: 1
	// timestamp: 1600000, value: 0.1
}
