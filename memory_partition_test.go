package tstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_memoryPartition_InsertRows(t *testing.T) {
	tests := []struct {
		name               string
		memoryPartition    *memoryPartition
		rows               []Row
		wantErr            bool
		wantDataPoints     []*DataPoint
		wantOutOfOrderRows []Row
	}{
		{
			name:            "insert in-order rows",
			memoryPartition: newMemoryPartition(nil, 0, "").(*memoryPartition),
			rows: []Row{
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: 1, Value: 0.1}},
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: 2, Value: 0.1}},
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: 3, Value: 0.1}},
			},
			wantDataPoints: []*DataPoint{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{},
		},
		{
			name: "insert out-of-order rows",
			memoryPartition: func() *memoryPartition {
				m := newMemoryPartition(nil, 0, "").(*memoryPartition)
				m.insertRows([]Row{
					{Metric: "metric1", DataPoint: DataPoint{Timestamp: 2, Value: 0.1}},
				})
				return m
			}(),
			rows: []Row{
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: 1, Value: 0.1}},
			},
			wantDataPoints: []*DataPoint{
				{Timestamp: 2, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: 1, Value: 0.1}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutOfOrder, err := tt.memoryPartition.insertRows(tt.rows)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantOutOfOrderRows, gotOutOfOrder)

			got := tt.memoryPartition.selectRows("metric1", nil, 0, 4)
			assert.Equal(t, tt.wantDataPoints, got)
		})
	}
}

func Test_memoryPartition_SelectRows(t *testing.T) {
	tests := []struct {
		name            string
		metric          string
		labels          []Label
		start           int64
		end             int64
		memoryPartition *memoryPartition
		want            []*DataPoint
	}{
		{
			name:            "given non-exist metric name",
			metric:          "unknown",
			start:           1,
			end:             2,
			memoryPartition: newMemoryPartition(nil, 0, "").(*memoryPartition),
			want:            []*DataPoint{},
		},
		{
			name:   "select multiple points",
			metric: "metric1",
			start:  1,
			end:    4,
			memoryPartition: func() *memoryPartition {
				m := newMemoryPartition(nil, 0, "").(*memoryPartition)
				m.insertRows([]Row{
					{
						Metric: "metric1",
						DataPoint: DataPoint{
							Timestamp: 1,
							Value:     0.1,
						},
					},
					{
						Metric: "metric1",
						DataPoint: DataPoint{
							Timestamp: 2,
							Value:     0.1,
						},
					},
					{
						Metric: "metric1",
						DataPoint: DataPoint{
							Timestamp: 3,
							Value:     0.1,
						},
					},
				})
				return m
			}(),
			want: []*DataPoint{
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
			got := tt.memoryPartition.selectRows(tt.metric, tt.labels, tt.start, tt.end)
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
			got := tt.memoryPartition.selectAll()
			assert.Equal(t, tt.want, got)
		})
	}
}
*/

func Test_toUnix(t *testing.T) {
	tests := []struct {
		name      string
		t         time.Time
		precision TimestampPrecision
		want      int64
	}{
		{
			name:      "to nanosecond",
			t:         time.Unix(1600000000, 0),
			precision: Nanoseconds,
			want:      1600000000000000000,
		},
		{
			name:      "to microsecond",
			t:         time.Unix(1600000000, 0),
			precision: Microseconds,
			want:      1600000000000000,
		},
		{
			name:      "to millisecond",
			t:         time.Unix(1600000000, 0),
			precision: Milliseconds,
			want:      1600000000000,
		},
		{
			name:      "to second",
			t:         time.Unix(1600000000, 0),
			precision: Seconds,
			want:      1600000000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toUnix(tt.t, tt.precision)
			assert.Equal(t, tt.want, got)
		})
	}
}
