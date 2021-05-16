package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_dataPointListImpl_insert(t *testing.T) {
	tests := []struct {
		name          string
		dataPointList dataPointList
		point         *DataPoint
		want          []DataPoint
	}{
		{
			name:          "first insertion",
			dataPointList: &dataPointListImpl{},
			point: &DataPoint{
				Timestamp: 1,
				Value:     0.1,
			},
			want: []DataPoint{
				{
					Timestamp: 1,
					Value:     0.1,
				},
			},
		},
		{
			name: "third insertion",
			dataPointList: func() dataPointList {
				node1 := &dataPointNode{
					val: &DataPoint{
						Timestamp: 1,
						Value:     0.1,
					},
				}
				node2 := &dataPointNode{
					val: &DataPoint{
						Timestamp: 2,
						Value:     0.1,
					},
				}
				node1.next = node2
				node2.prev = node1
				return newDataPointList(node1, node2, 2)
			}(),
			point: &DataPoint{
				Timestamp: 3,
				Value:     0.1,
			},
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
			tt.dataPointList.insert(tt.point)
			iterator := tt.dataPointList.newIterator()
			got := []DataPoint{}
			for iterator.Next() {
				got = append(got, *iterator.DataPoint())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
