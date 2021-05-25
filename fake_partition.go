package tstorage

type fakePartition struct {
	minT      int64
	maxT      int64
	numPoints int
	IsActive  bool

	err error
}

func (f *fakePartition) insertRows(_ []Row) ([]Row, error) {
	return nil, f.err
}

func (f *fakePartition) selectRows(_ string, _ []Label, _, _ int64) []*DataPoint {
	return nil
}

func (f *fakePartition) selectAll() []Row {
	return nil
}

func (f *fakePartition) minTimestamp() int64 {
	return f.minT
}

func (f *fakePartition) maxTimestamp() int64 {
	return f.maxT
}

func (f *fakePartition) size() int {
	return f.numPoints
}

func (f *fakePartition) active() bool {
	return f.IsActive
}
