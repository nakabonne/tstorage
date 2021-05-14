package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nakabonne/tsdbe/partition"
	"github.com/nakabonne/tsdbe/partition/memory"
)

func Test_storage_SelectRows(t *testing.T) {
	tests := []struct {
		name       string
		storage    storage
		metricName string
		start      int64
		end        int64
		want       []partition.DataPoint
	}{
		{
			name: "select from three partitions",
			storage: func() storage {
				part1 := memory.NewMemoryPartition(nil, 1*time.Hour)
				err := part1.InsertRows([]partition.Row{
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 1},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 2},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 3},
					},
				})
				if err != nil {
					panic(err)
				}
				part2 := memory.NewMemoryPartition(nil, 1*time.Hour)
				err = part2.InsertRows([]partition.Row{
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 4},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 5},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 6},
					},
				})
				if err != nil {
					panic(err)
				}
				part3 := memory.NewMemoryPartition(nil, 1*time.Hour)
				err = part3.InsertRows([]partition.Row{
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 7},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 8},
					},
					{
						MetricName: "metric1",
						DataPoint:  partition.DataPoint{Timestamp: 9},
					},
				})
				if err != nil {
					panic(err)
				}
				list := partition.NewPartitionList()
				list.Insert(part1)
				list.Insert(part2)
				list.Insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			metricName: "metric1",
			start:      0,
			end:        10,
			want: []partition.DataPoint{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 4,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 6,
				},
				{
					Timestamp: 7,
				},
				{
					Timestamp: 8,
				},
				{
					Timestamp: 9,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.storage.SelectRows(tt.metricName, tt.start, tt.end)
			assert.Equal(t, tt.want, got)
		})
	}
}
