package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalMetricName(t *testing.T) {
	tests := []struct {
		name   string
		labels []Label
		want   string
	}{
		{
			name: "only __name__ label",
			labels: []Label{
				{
					Name:  "__name__",
					Value: "metric1",
				},
			},
			want: "\x00\b__name__\x00\ametric1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalMetricName(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}
