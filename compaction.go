// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tstorage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

type seriesEncoder interface {
	encodePoints(points []*DataPoint) error
	compress() error
}

func newSeriesEncoder(w io.Writer) seriesEncoder {
	return &gorillaEncoder{
		w:   w,
		buf: new(bytes.Buffer),
	}
}

type gorillaEncoder struct {
	// backend stream writer
	w io.Writer

	// buffer to be used while encoding
	buf *bytes.Buffer
}

// encodePoints is not goroutine safe. It's caller's responsibility to lock it.
func (e *gorillaEncoder) encodePoints(points []*DataPoint) error {
	// FIXME: Implement gorilla encoding

	for i := range points {
		if err := binary.Write(e.buf, binary.LittleEndian, points[i]); err != nil {
			return err
		}
	}
	return nil
}

// compress compress the buffered-date and writes them into the backend io.Writer
func (e *gorillaEncoder) compress() error {
	// FIXME: Compress with ZStandard instead of gzip

	gzipWriter := gzip.NewWriter(e.w)
	if _, err := gzipWriter.Write(e.buf.Bytes()); err != nil {
		return err
	}
	return gzipWriter.Close()
}

type seriesDecoder interface {
	decodePoint(dst *DataPoint) error
	seek(offset int64) (int64, error)
}

// newSeriesDecoder decompress data from the given Reader, then holds the decompressed data
func newSeriesDecoder(r io.Reader) (seriesDecoder, error) {
	// FIXME: Decompress with ZStandard instead of gzip

	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to new gzip reader: %w", err)
	}

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, gzipReader); err != nil {
		return nil, fmt.Errorf("failed to copy bytes: %w", err)
	}
	if err := gzipReader.Close(); err != nil {
		return nil, fmt.Errorf("failed to close: %w", err)
	}
	return &gorillaDecoder{r: bytes.NewReader(buf.Bytes())}, nil
}

type gorillaDecoder struct {
	r io.ReadSeeker
}

func (d *gorillaDecoder) seek(offset int64) (int64, error) {
	return d.r.Seek(offset, 0)
}

func (d *gorillaDecoder) decodePoint(dst *DataPoint) error {
	// FIXME: Implement gorilla decoding

	return binary.Read(d.r, binary.LittleEndian, dst)
}
