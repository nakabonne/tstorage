package tstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_storage_SelectRows(t *testing.T) {
	tests := []struct {
		name       string
		storage    storage
		metricName string
		start      int64
		end        int64
		want       []DataPoint
		wantSize   int
		wantErr    bool
	}{
		{
			name:       "select from single partition",
			metricName: "\x00\b__name__\x00\ametric1",
			start:      0,
			end:        4,
			storage: func() storage {
				part1 := NewMemoryPartition(nil, 1*time.Hour)
				err := part1.InsertRows([]Row{
					{
						DataPoint: DataPoint{Timestamp: 1},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 2},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 3},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
				})
				if err != nil {
					panic(err)
				}
				list := NewPartitionList()
				list.Insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []DataPoint{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
			},
			wantSize: 3,
		},
		{
			name:       "select from three partitions",
			metricName: "\x00\b__name__\x00\ametric1",
			start:      0,
			end:        10,
			storage: func() storage {
				part1 := NewMemoryPartition(nil, 1*time.Hour)
				err := part1.InsertRows([]Row{
					{
						DataPoint: DataPoint{Timestamp: 1},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 2},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 3},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
				})
				if err != nil {
					panic(err)
				}
				part2 := NewMemoryPartition(nil, 1*time.Hour)
				err = part2.InsertRows([]Row{
					{
						DataPoint: DataPoint{Timestamp: 4},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 5},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 6},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
				})
				if err != nil {
					panic(err)
				}
				part3 := NewMemoryPartition(nil, 1*time.Hour)
				err = part3.InsertRows([]Row{
					{
						DataPoint: DataPoint{Timestamp: 7},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 8},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
					{
						DataPoint: DataPoint{Timestamp: 9},
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
					},
				})
				if err != nil {
					panic(err)
				}
				list := NewPartitionList()
				list.Insert(part1)
				list.Insert(part2)
				list.Insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []DataPoint{
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
			wantSize: 9,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iterator, gotSize, err := tt.storage.SelectRows(tt.metricName, tt.start, tt.end)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantSize, gotSize)
			got := []DataPoint{}
			for iterator.Next() {
				got = append(got, *iterator.Value())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
