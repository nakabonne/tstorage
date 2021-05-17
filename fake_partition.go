package tstorage

type fakePartition struct {
	minTimestamp int64
	maxTimestamp int64
	size         int
	readOnly     bool

	err error
}

func (f *fakePartition) insertRows(_ []Row) error {
	return f.err
}

func (f *fakePartition) selectRows(_ string, _ []Label, _, _ int64) dataPointList {
	return nil
}

func (f *fakePartition) SelectAll() []Row {
	return nil
}

func (f *fakePartition) MinTimestamp() int64 {
	return f.minTimestamp
}

func (f *fakePartition) MaxTimestamp() int64 {
	return f.maxTimestamp
}

func (f *fakePartition) Size() int {
	return f.size
}

func (f *fakePartition) ReadOnly() bool {
	return f.readOnly
}
