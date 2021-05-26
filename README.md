# tstorage [![Go Reference](https://pkg.go.dev/badge/mod/github.com/nakabonne/tstorage.svg)](https://pkg.go.dev/mod/github.com/nakabonne/tstorage)

**NOTE: This package is under development. It's not ready for you to use.**

`tstorage` is a fast local in-memory/on-disk storage package for time-series data with a straightforward API.
It massively optimises ingestion as it allows the database to slice data extremely efficiently in small chunks and process it all in parallel.

## Usage
The below example illustrates how to insert a row into the process memory and immediately select it.

```go
package main

import (
	"fmt"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, _ := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	_ = storage.InsertRows([]tstorage.Row{
		{Metric: "metric1", DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
	})
	points, _ := storage.SelectDataPoints("metric1", nil, 1600000000, 1600000001)
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		// => timestamp: 1600000000, value: 0.1
	}
}
```

### Examples
In tstorage, the combination of metric name and labels preserves uniqueness.
Here is an example of insertion and selection a metric for memory allocation on `host-1`.

```go
package main

import (
	"fmt"

	"github.com/nakabonne/tstorage"
)

func main() {
	storage, _ := tstorage.NewStorage(
		tstorage.WithDataPath("./data"),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	_ = storage.InsertRows([]tstorage.Row{
		{
			Metric: "mem_alloc_bytes",
			Labels: []tstorage.Label{
				{Name: "host", Value: "host-1"},
			},
			DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1}},
	})
	points, _ := storage.SelectDataPoints(
		"mem_alloc_bytes",
		[]tstorage.Label{{Name: "host", Value: "host-1"}},
		1600000000, 1600000001,
	)
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		// => timestamp: 1600000000, value: 0.1
	}
}
```


For more examples see [the documentation](https://pkg.go.dev/github.com/nakabonne/tstorage#pkg-examples).

## Internal
Time-series database has specific characteristics in its workload.
In terms of write operations, a time-series database must be designed to handle exceptionally large volumes, specifically, performant ingestion is a cornerstone feature.
In terms of read operations, most recent first. In most cases, users want to query in real-time. Databases should be able to pull the latest record very fast, easily.
Entirely, time-series data is mostly an append-only workload with delete operations performed in batches on less recent data.

Based on these characteristics, `tstorage`'s data model differs from the B-trees or LSM trees based storage engines.
This package adopts a linear data model structure which partitions data points by time.
Each partition acts as a fully independent database containing all data points for its time range.

![Screenshot](architecture.jpg)

Key benefits:
- When querying a time range, we can easily ignore all data outside of the partition range.
- When completing a partition, we can persist the data from our in-memory database by sequentially writing just a handful of larger files. We avoid any write-amplification and serve SSDs and HDDs equally well.

## Benchmarks
Benchmark tests were made using Intel(R) Core(TM) i7-8559U CPU @ 2.70GHz with 16GB of RAM on macOS 10.15.7

```
$ go version
go version go1.16.2 darwin/amd64

$ go test -benchtime=4s -benchmem -bench=. .
goos: darwin
goarch: amd64
pkg: github.com/nakabonne/tstorage
cpu: Intel(R) Core(TM) i7-8559U CPU @ 2.70GHz
BenchmarkStorage_InsertRows-8                            	15981140	       303.4 ns/op	     169 B/op	       2 allocs/op
BenchmarkStorage_SelectDataPointsAmongThousandPoints-8   	21974707	       217.5 ns/op	      56 B/op	       2 allocs/op
BenchmarkStorage_SelectDataPointsAmongMillionPoints-8    	16951826	       282.4 ns/op	      55 B/op	       1 allocs/op
PASS
ok  	github.com/nakabonne/tstorage	17.166s
```

## Used by
- [ali](https://github.com/nakabonne/ali) - A load testing tool capable of performing real-time analysis
