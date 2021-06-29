package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// FIXME: Test for gorilla

func Test_bitRange(t *testing.T) {
	tests := []struct {
		name  string
		x     int64
		nbits uint8
		want  bool
	}{
		{
			name:  "inside the range",
			x:     1,
			nbits: 1,
			want:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bitRange(tt.x, tt.nbits)
			assert.Equal(t, tt.want, got)
		})
	}
}
