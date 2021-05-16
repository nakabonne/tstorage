package tstorage

var _ MemoryPartition = &redisPartition{}

type redisPartition struct {
}

func (r redisPartition) InsertRows(rows []Row) error {
	panic("implement me")
}

func (r redisPartition) SelectRows(metricName string, start, end int64) dataPointList {
	panic("implement me")
}

func (r redisPartition) SelectAll() []Row {
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
