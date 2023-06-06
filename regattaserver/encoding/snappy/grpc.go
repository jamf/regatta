// Copyright JAMF Software, LLC

package snappy

import (
	"io"
	"sync"

	gs "github.com/klauspost/compress/snappy"
	"google.golang.org/grpc/encoding"
)

const Name = "snappy"

func init() {
	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		w := gs.NewBufferedWriter(io.Discard)
		return &writer{Writer: w, pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*gs.Writer
	pool *sync.Pool
}

type reader struct {
	*gs.Reader
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
		newR := gs.NewReader(r)
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
