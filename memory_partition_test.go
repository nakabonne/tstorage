package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_memoryPartition_InsertRows(t *testing.T) {
	tests := []struct {
		name            string
		memoryPartition memoryPartition
		rows            []Row
		wantErr         bool
		wantDataPoints  []DataPoint
	}{
		{
			name:            "insert multiple rows",
			memoryPartition: memoryPartition{},
			rows: []Row{
				{
					Labels: []Label{
						{
							Name:  "__name__",
							Value: "metric1",
						},
					},
					DataPoint: DataPoint{
						Timestamp: 1,
						Value:     0.1,
					},
				},
				{
					Labels: []Label{
						{
							Name:  "__name__",
							Value: "metric1",
						},
					},
					DataPoint: DataPoint{
						Timestamp: 2,
						Value:     0.1,
					},
				},
				{
					Labels: []Label{
						{
							Name:  "__name__",
							Value: "metric1",
						},
					},
					DataPoint: DataPoint{
						Timestamp: 3,
						Value:     0.1,
					},
				},
			},
			wantDataPoints: []DataPoint{
				{
					Timestamp: 1,
					Value:     0.1,
				},
				{
					Timestamp: 2,
					Value:     0.1,
				},
				{
					Timestamp: 3,
					Value:     0.1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.memoryPartition.InsertRows(tt.rows)
			assert.Equal(t, tt.wantErr, err != nil)

			list := tt.memoryPartition.SelectRows([]Label{
				{
					Name:  "__name__",
					Value: "metric1",
				},
			}, 0, 4)
			iterator := list.newIterator()
			got := []DataPoint{}
			for iterator.Next() {
				got = append(got, *iterator.Value())
			}
			assert.Equal(t, tt.wantDataPoints, got)
		})
	}
}

func Test_memoryPartition_SelectRows(t *testing.T) {
	tests := []struct {
		name            string
		labels          []Label
		start           int64
		end             int64
		memoryPartition memoryPartition
		want            []DataPoint
	}{
		{
			name: "given non-exist metric name",
			labels: []Label{
				{
					Name:  "__name__",
					Value: "unknow",
				},
			},
			start:           1,
			end:             2,
			memoryPartition: memoryPartition{},
			want:            []DataPoint{},
		},
		{
			name: "select multiple points",
			labels: []Label{
				{
					Name:  "__name__",
					Value: "metric1",
				},
			},
			start: 0,
			end:   3,
			memoryPartition: func() memoryPartition {
				m := memoryPartition{}
				m.InsertRows([]Row{
					{
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
						DataPoint: DataPoint{
							Timestamp: 1,
							Value:     0.1,
						},
					},
					{
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
						DataPoint: DataPoint{
							Timestamp: 2,
							Value:     0.1,
						},
					},
					{
						Labels: []Label{
							{
								Name:  "__name__",
								Value: "metric1",
							},
						},
						DataPoint: DataPoint{
							Timestamp: 3,
							Value:     0.1,
						},
					},
				})
				return m
			}(),
			want: []DataPoint{
				{
					Timestamp: 1,
					Value:     0.1,
				},
				{
					Timestamp: 2,
					Value:     0.1,
				},
				{
					Timestamp: 3,
					Value:     0.1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := tt.memoryPartition.SelectRows(tt.labels, tt.start, tt.end)
			iterator := list.newIterator()
			got := []DataPoint{}
			for iterator.Next() {
				got = append(got, *iterator.Value())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

/*
func TestSelectAll(t *testing.T) {
	tests := []struct {
		name            string
		memoryPartition memoryPartition
		want            []Row
	}{
		{
			name: "single data point for single metric",
			memoryPartition: func() memoryPartition {
				m := memoryPartition{}
				m.metrics.Store("metric1", &metric{
					name: "metric1",
					points: []DataPoint{
						{
							Timestamp: 1,
							value:     0.1,
						},
					},
				})
				return m
			}(),
			want: []Row{
				{
					//MetricName: "metric1",
					DataPoint: DataPoint{
						Timestamp: 1,
						value:     0.1,
					},
				},
			},
		},
		{
			name: "multiple data points for multiple metrics",
			memoryPartition: func() memoryPartition {
				m := memoryPartition{}
				m.metrics.Store("metric1", &metric{
					name: "metric1",
					points: []DataPoint{
						{
							Timestamp: 1,
							value:     0.1,
						},
						{
							Timestamp: 2,
							value:     0.2,
						},
					},
				})
				m.metrics.Store("metric2", &metric{
					name: "metric2",
					points: []DataPoint{
						{
							Timestamp: 1,
							value:     0.1,
						},
						{
							Timestamp: 2,
							value:     0.2,
						},
					},
				})
				return m
			}(),
			want: []Row{
				{
					//MetricName: "metric1",
					DataPoint: DataPoint{
						Timestamp: 1,
						value:     0.1,
					},
				},
				{
					//MetricName: "metric1",
					DataPoint: DataPoint{
						Timestamp: 2,
						value:     0.2,
					},
				},
				{
					//MetricName: "metric2",
					DataPoint: DataPoint{
						Timestamp: 1,
						value:     0.1,
					},
				},
				{
					//MetricName: "metric2",
					DataPoint: DataPoint{
						Timestamp: 2,
						value:     0.2,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.memoryPartition.SelectAll()
			assert.Equal(t, tt.want, got)
		})
	}
}
*/
