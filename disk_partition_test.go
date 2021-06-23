package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDiskPartition(t *testing.T) {
	tests := []struct {
		name    string
		dirPath string
		want    partition
		wantErr bool
	}{
		{
			name:    "empty dir name given",
			dirPath: "",
			wantErr: true,
		},
		{
			name:    "non-existent dir given",
			dirPath: "./non-existent-dir",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := openDiskPartition(tt.dirPath)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
		})
	}
}
