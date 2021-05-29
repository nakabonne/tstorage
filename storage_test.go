package tstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_storage_Select(t *testing.T) {
	tests := []struct {
		name    string
		storage storage
		metric  string
		labels  []Label
		start   int64
		end     int64
		want    []*DataPoint
		wantErr bool
	}{
		{
			name:   "select from single partition",
			metric: "metric1",
			start:  1,
			end:    4,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
		},
		{
			name:   "select from three partitions",
			metric: "metric1",
			start:  1,
			end:    10,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
				{Timestamp: 4},
				{Timestamp: 5},
				{Timestamp: 6},
				{Timestamp: 7},
				{Timestamp: 8},
				{Timestamp: 9},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.storage.Select(tt.metric, tt.labels, tt.start, tt.end)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, got)
		})
	}
}
