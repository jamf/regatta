// Copyright JAMF Software, LLC

package gzip

import (
	"io"
	"sync"

	gz "github.com/klauspost/compress/gzip"
	"google.golang.org/grpc/encoding"
)

const Name = "gzip"

func init() {
	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		w, err := gz.NewWriterLevel(io.Discard, gz.BestSpeed)
		if err != nil {
			panic(err)
		}
		return &writer{Writer: w, pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*gz.Writer
	pool *sync.Pool
}

type reader struct {
	*gz.Reader
	pool *sync.Pool
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	z := c.poolCompressor.Get().(*writer)
	z.Writer.Reset(w)
	return z, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	z, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		newR, err := gz.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &reader{Reader: newR, pool: &c.poolDecompressor}, nil
	}
	z.Reset(r)
	return z, nil
}

func (c *compressor) Name() string {
	return Name
}

func (z *writer) Close() error {
	err := z.Writer.Close()
	z.pool.Put(z)
	return err
}

func (z *reader) Read(p []byte) (n int, err error) {
	n, err = z.Reader.Read(p)
	if err == io.EOF {
		z.pool.Put(z)
	}
	return n, err
}
