package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nakabonne/fiatdb/app/fiatd/storage/partition"
)

func TestOpenDiskPartition(t *testing.T) {
	tests := []struct {
		name    string
		dirPath string
		want    partition.Partition
		wantErr bool
	}{
		{
			name:    "existent dir given",
			dirPath: "./testdata",
			want: &diskPartition{
				dirPath:      "./testdata",
				size:         2,
				minTimestamp: 1600000,
				maxTimestamp: 1600001,
			},
		},
		{
			name:    "non-existent dir given",
			dirPath: "./non-existent-dir",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenDiskPartition(tt.dirPath)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
		})
	}
}
