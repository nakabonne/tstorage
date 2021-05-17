# tstorage
TStorage is a fast local on-disk storage package for time-series data with a straightforward API.
It is massively optimized ingestion as it allows the database to slice data extremely efficiently in small chunks and process it all in parallel.

## Usage

```go
package main

import (
	"fmt"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, _ := tstorage.NewStorage()
	_ = storage.InsertRows([]tstorage.Row{
		{
			Metric: "metric1",
			DataPoint: tstorage.DataPoint{
				Timestamp: 1600000,
				Value:     0.1,
			},
		},
	})
	iterator, _, _ := storage.SelectRows("metric1", nil, 1600000, 1600001)
	for iterator.Next() {
		fmt.Printf("timestamp: %v, value: %v\n", iterator.DataPoint().Timestamp, iterator.DataPoint().Value)
		// => timestamp: 1600000, value: 0.1
	}
}
```

For more examples see [here](https://pkg.go.dev/github.com/nakabonne/tstorage#pkg-examples).

## Who's it for?
For those who want
- a fast time-series storage engine

## Internal
TStorage's data model differs from the B-trees or LSM trees based storage engines.

## Used by
- [ali](https://github.com/nakabonne/ali) - A load testing tool capable of performing real-time analysis