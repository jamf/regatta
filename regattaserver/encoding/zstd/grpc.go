// Copyright JAMF Software, LLC

package zstd

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

const Name = "zstd"

func init() {
	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		w, _ := zstd.NewWriter(io.Discard)
		return &writer{Encoder: w, pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*zstd.Encoder
	pool *sync.Pool
}

type reader struct {
	*zstd.Decoder
	pool *sync.Pool
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	z := c.poolCompressor.Get().(*writer)
	z.Encoder.Reset(w)
	return z, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	z, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		newR, _ := zstd.NewReader(r)
		return &reader{Decoder: newR, pool: &c.poolDecompressor}, nil
	}
	z.Reset(r)
	return z, nil
}

func (c *compressor) Name() string {
	return Name
}

func (z *writer) Close() error {
	err := z.Encoder.Close()
	z.pool.Put(z)
	return err
}

func (z *reader) Read(p []byte) (n int, err error) {
	n, err = z.Decoder.Read(p)
	if err == io.EOF {
		z.pool.Put(z)
	}
	return n, err
}
