package tstorage

type fakeEncoder struct {
	encodePointFunc func(*DataPoint) error
	flushFunc       func() error
}

func (f *fakeEncoder) encodePoint(p *DataPoint) error {
	return f.encodePointFunc(p)
}

func (f *fakeEncoder) flush() error {
	return f.flushFunc()
}
