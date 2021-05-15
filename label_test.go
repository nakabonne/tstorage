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
					Name:  []byte(""),
					Value: []byte("metric_a"),
				},
			},
			want: "\x00\x00\x00\bmetric_a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalMetricName(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}
