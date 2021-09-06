package tstorage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			got, _ := tt.memoryPartition.selectDataPoints("metric1", nil, 0, 4)
			assert.Equal(t, tt.wantDataPoints, got)
		})
	}
}

func Test_memoryPartition_SelectDataPoints(t *testing.T) {
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
			name:   "select some points",
			metric: "metric1",
			start:  2,
			end:    4,
			memoryPartition: func() *memoryPartition {
				m := newMemoryPartition(nil, 0, "").(*memoryPartition)
				m.insertRows([]Row{
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 1, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 2, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 3, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 4, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 5, Value: 0.1},
					},
				})
				return m
			}(),
			want: []*DataPoint{
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
		},
		{
			name:   "select all points",
			metric: "metric1",
			start:  1,
			end:    4,
			memoryPartition: func() *memoryPartition {
				m := newMemoryPartition(nil, 0, "").(*memoryPartition)
				m.insertRows([]Row{
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 1, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 2, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint{Timestamp: 3, Value: 0.1},
					},
				})
				return m
			}(),
			want: []*DataPoint{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tt.memoryPartition.selectDataPoints(tt.metric, tt.labels, tt.start, tt.end)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_memoryMetric_EncodeAllPoints_sorted(t *testing.T) {
	mt := memoryMetric{
		points: []*DataPoint{
			{Timestamp: 1, Value: 0.1},
			{Timestamp: 3, Value: 0.1},
		},
		outOfOrderPoints: []*DataPoint{
			{Timestamp: 4, Value: 0.1},
			{Timestamp: 2, Value: 0.1},
		},
	}
	allTimestamps := make([]int64, 0, 4)
	encoder := fakeEncoder{
		encodePointFunc: func(p *DataPoint) error {
			allTimestamps = append(allTimestamps, p.Timestamp)
			return nil
		},
	}
	err := mt.encodeAllPoints(&encoder)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3, 4}, allTimestamps)
}

func Test_memoryMetric_EncodeAllPoints_error(t *testing.T) {
	mt := memoryMetric{
		points: []*DataPoint{{Timestamp: 1, Value: 0.1}},
	}
	encoder := fakeEncoder{
		encodePointFunc: func(p *DataPoint) error {
			return fmt.Errorf("some error")
		},
	}
	err := mt.encodeAllPoints(&encoder)
	assert.Error(t, err)
}

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
