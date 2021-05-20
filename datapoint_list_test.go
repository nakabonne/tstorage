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
		wantSize      int
	}{
		{
			name:          "first insertion",
			dataPointList: &dataPointListImpl{},
			point:         &DataPoint{Timestamp: 1},
			want: []DataPoint{
				{Timestamp: 1},
			},
			wantSize: 1,
		},
		{
			name: "insert in order data",
			dataPointList: func() dataPointList {
				node1 := &dataPointNode{
					val: &DataPoint{Timestamp: 1},
				}
				node2 := &dataPointNode{
					val: &DataPoint{Timestamp: 2},
				}
				node1.next = node2
				node2.prev = node1
				return newDataPointList(node1, node2, 2)
			}(),
			point: &DataPoint{Timestamp: 3},
			want: []DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
			wantSize: 3,
		},
		{
			name: "insert out of order data into the middle",
			dataPointList: func() dataPointList {
				node1 := &dataPointNode{
					val: &DataPoint{Timestamp: 1},
				}
				node2 := &dataPointNode{
					val: &DataPoint{Timestamp: 3},
				}
				node3 := &dataPointNode{
					val: &DataPoint{Timestamp: 4},
				}
				node1.next = node2
				node2.prev = node1
				node2.next = node3
				node3.prev = node2
				return newDataPointList(node1, node3, 3)
			}(),
			point: &DataPoint{Timestamp: 2},
			want: []DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
				{Timestamp: 4},
			},
			wantSize: 4,
		},
		{
			name: "insert out of order data into the head",
			dataPointList: func() dataPointList {
				node1 := &dataPointNode{
					val: &DataPoint{Timestamp: 1},
				}
				node2 := &dataPointNode{
					val: &DataPoint{Timestamp: 2},
				}
				node3 := &dataPointNode{
					val: &DataPoint{Timestamp: 3},
				}
				node1.next = node2
				node2.prev = node1
				node2.next = node3
				node3.prev = node2
				return newDataPointList(node1, node3, 3)
			}(),
			point: &DataPoint{Timestamp: 0},
			want: []DataPoint{
				{Timestamp: 0},
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
			wantSize: 4,
		},
		{
			name: "insert data point as same as head",
			dataPointList: func() dataPointList {
				node1 := &dataPointNode{
					val: &DataPoint{Timestamp: 1},
				}
				node2 := &dataPointNode{
					val: &DataPoint{Timestamp: 2},
				}
				node3 := &dataPointNode{
					val: &DataPoint{Timestamp: 3},
				}
				node1.next = node2
				node2.prev = node1
				node2.next = node3
				node3.prev = node2
				return newDataPointList(node1, node3, 3)
			}(),
			point: &DataPoint{Timestamp: 1},
			want: []DataPoint{
				{Timestamp: 1},
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
			wantSize: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.dataPointList.insert(tt.point)
			assert.Equal(t, tt.wantSize, tt.dataPointList.size())
			iterator := tt.dataPointList.newIterator()
			got := []DataPoint{}
			for iterator.Next() {
				got = append(got, *iterator.DataPoint())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
