package tstorage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_gorillaEncoder_encodePoint_decodePoint(t *testing.T) {
	tests := []struct {
		name                string
		input               []*DataPoint // to be encoded
		want                []*DataPoint
		wantEncodedByteSize int
		wantErr             bool
	}{
		{
			name: "one data point",
			input: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
			},
			wantEncodedByteSize: 14,
			wantErr:             false,
		},
		{
			name: "data points at regular intervals",
			input: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
				{Timestamp: 1600000060, Value: 0.1},
				{Timestamp: 1600000120, Value: 0.1},
				{Timestamp: 1600000180, Value: 0.1},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
				{Timestamp: 1600000060, Value: 0.1},
				{Timestamp: 1600000120, Value: 0.1},
				{Timestamp: 1600000180, Value: 0.1},
			},
			wantEncodedByteSize: 15,
			wantErr:             false,
		},
		{
			name: "data points at random intervals",
			input: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
				{Timestamp: 1600000060, Value: 1.1},
				{Timestamp: 1600000182, Value: 15.01},
				{Timestamp: 1600000400, Value: 0.01},
				{Timestamp: 1600002000, Value: 10.8},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Value: 0.1},
				{Timestamp: 1600000060, Value: 1.1},
				{Timestamp: 1600000182, Value: 15.01},
				{Timestamp: 1600000400, Value: 0.01},
				{Timestamp: 1600002000, Value: 10.8},
			},
			wantEncodedByteSize: 52,
			wantErr:             false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			var num int
			encoder := newSeriesEncoder(&buf)
			for _, point := range tt.input {
				err := encoder.encodePoint(point)
				require.NoError(t, err)
				num++
			}
			err := encoder.flush()
			require.NoError(t, err)

			assert.Equal(t, tt.wantEncodedByteSize, buf.Len())

			// Decode
			decoder, err := newSeriesDecoder(&buf)
			require.NoError(t, err)
			got := make([]*DataPoint, 0, num)
			for i := 0; i < num; i++ {
				p := &DataPoint{}
				err := decoder.decodePoint(p)
				require.NoError(t, err)
				got = append(got, p)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

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
