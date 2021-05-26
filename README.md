# tstorage
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/mod/github.com/nakabonne/tstorage?tab=packages)

`tstorage` is a fast local on-disk storage package for time-series data with a straightforward API.
It is massively optimized ingestion as it allows the database to slice data extremely efficiently in small chunks and process it all in parallel.

## Usage

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

For more examples see [the documentation](https://pkg.go.dev/github.com/nakabonne/tstorage#pkg-examples).

## Internal
Time-series data has specific characteristics in its workload.
In terms of write operations, a time-series database must be designed to handle exceptionally large volumes, specifically, performant ingestion is a cornerstone feature.
In terms of read operations, most recent first. In most cases, users want to query in real-time. Databases should be able to pull the latest record very fast, easily.
Entirely, time-series data is mostly an append-only workload with delete operations performed in batches on less recent data.

Based on this characteristics, `tstorage`'s data model differs from the B-trees or LSM trees based storage engines.
This package adopts linear data model structure which partitions data points by time.
Each partition acts as a fully independent database containing all data points for its time range.

Benefits:
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
BenchmarkStorage_InsertRows-8                           	13673982	       323.7 ns/op	     176 B/op	       2 allocs/op
BenchmarkStorage_SelectDataPointsFromThousandPoints-8   	15553446	       295.3 ns/op	      56 B/op	       2 allocs/op
BenchmarkStorage_SelectDataPointsFromMillionPoints-8    	15637478	       294.8 ns/op	      56 B/op	       1 allocs/op
PASS
ok  	github.com/nakabonne/tstorage	16.478s
```

## Used by
- [ali](https://github.com/nakabonne/ali) - A load testing tool capable of performing real-time analysis
