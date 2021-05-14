package redis

import "github.com/nakabonne/fiatdb/app/fiatd/storage/partition"

var _ partition.MemoryPartition = &redisPartition{}

type redisPartition struct {
}

func (r redisPartition) InsertRows(rows []partition.Row) error {
	panic("implement me")
}

func (r redisPartition) SelectRows(metricName string, start, end int64) []partition.DataPoint {
	panic("implement me")
}

func (r redisPartition) SelectAll() []partition.Row {
	panic("implement me")
}

func (r redisPartition) MinTimestamp() int64 {
	panic("implement me")
}

func (r redisPartition) MaxTimestamp() int64 {
	panic("implement me")
}

func (r redisPartition) Size() int {
	panic("implement me")
}

func (r redisPartition) ReadOnly() bool {
	panic("implement me")
}

func (r redisPartition) ReadyToBePersisted() bool {
	panic("implement me")
}
