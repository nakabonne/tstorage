package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalMetricName(t *testing.T) {
	tests := []struct {
		name   string
		metric string
		labels []Label
		want   string
	}{
		{
			name:   "only metric",
			metric: "metric1",
			want:   "metric1",
		},
		{
			name:   "missing label name",
			metric: "metric1",
			labels: []Label{
				{Value: "value1"},
			},

			want: "\x00\ametric1",
		},
		{
			name:   "missing label value",
			metric: "metric1",
			labels: []Label{
				{Name: "metric1"},
			},

			want: "\x00\ametric1",
		},
		{
			name:   "metric with a single label",
			metric: "metric1",
			labels: []Label{
				{Name: "name1", Value: "value1"},
			},
			want: "\x00\ametric1\x00\x05name1\x00\x06value1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := marshalMetricName(tt.metric, tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}
