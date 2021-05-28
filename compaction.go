package tstorage

import (
	"compress/gzip"
	"encoding/binary"
	"io"
)

type compressor interface {
	write(points []*DataPoint) error
	close() error
}
type decompressor interface {
	read(dst *DataPoint) error
	close() error
}

type gzipCompressor struct {
	writer *gzip.Writer
}

func newGzipCompressor(w io.WriteSeeker) compressor {
	return &gzipCompressor{writer: gzip.NewWriter(w)}
}

func (g *gzipCompressor) write(points []*DataPoint) error {
	for i := range points {
		if err := binary.Write(g.writer, binary.LittleEndian, points[i]); err != nil {
			return err
		}

		if err := g.writer.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (g *gzipCompressor) close() error {
	return g.writer.Close()
}

type gzipDecompressor struct {
	reader *gzip.Reader
}

func newGzipDecompressor(r io.Reader) (decompressor, error) {
	reader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &gzipDecompressor{reader: reader}, nil
}

func (g *gzipDecompressor) read(dst *DataPoint) error {
	if err := binary.Read(g.reader, binary.LittleEndian, dst); err != nil {
		return err
	}
	return nil
}

func (g *gzipDecompressor) close() error {
	return g.reader.Close()
}
