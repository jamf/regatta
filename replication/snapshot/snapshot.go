// Copyright JAMF Software, LLC

package snapshot

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/jamf/regatta/proto"
	"golang.org/x/time/rate"
)

// DefaultSnapshotChunkSize default chunk size of gRPC snapshot stream.
const DefaultSnapshotChunkSize = 1024 * 1024

const snapshotFilenamePattern = "snapshot-*.bin"

type Writer struct {
	Sender proto.Snapshot_StreamServer
}

func (g *Writer) ReadFrom(r io.Reader) (int64, error) {
	count := int64(0)
	chunk := make([]byte, DefaultSnapshotChunkSize)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			count += int64(n)
			if err := g.Sender.Send(&proto.SnapshotChunk{
				Data: chunk[:n],
				Len:  uint64(n),
			}); err != nil {
				return count, err
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, err
		}
	}
	return count, nil
}

func (g *Writer) Write(p []byte) (int, error) {
	ln := len(p)
	if err := g.Sender.Send(&proto.SnapshotChunk{
		Data: p,
		Len:  uint64(ln),
	}); err != nil {
		return 0, err
	}
	return ln, nil
}

type Reader struct {
	Stream  proto.Snapshot_StreamClient
	Limiter *rate.Limiter
}

func (s Reader) Read(p []byte) (int, error) {
	chunk := proto.SnapshotChunkFromVTPool()
	defer chunk.ReturnToVTPool()
	if err := s.Stream.RecvMsg(chunk); err != nil {
		return 0, err
	}
	if len(p) < int(chunk.Len) {
		return 0, io.ErrShortBuffer
	}
	if s.Limiter != nil {
		s.Limiter.WaitN(s.Stream.Context(), int(chunk.Len))
	}
	return copy(p, chunk.Data), nil
}

func (s Reader) WriteTo(w io.Writer) (int64, error) {
	n := int64(0)
	chunk := proto.SnapshotChunkFromVTPool()
	defer chunk.ReturnToVTPool()
	for {
		chunk.ResetVT()
		err := s.Stream.RecvMsg(chunk)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if s.Limiter != nil {
			s.Limiter.WaitN(s.Stream.Context(), int(chunk.Len))
		}
		w, err := w.Write(chunk.Data)
		if err != nil {
			return n, err
		}
		n += int64(w)
	}
}

func OpenFile(path string) (*snapshotFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return newFile(f, path), nil
}

func NewTemp() (*snapshotFile, error) {
	dir := os.TempDir()
	f, err := os.CreateTemp(dir, snapshotFilenamePattern)
	if err != nil {
		return nil, err
	}
	return newFile(f, f.Name()), nil
}

func newFile(file *os.File, path string) *snapshotFile {
	return &snapshotFile{
		File:    file,
		path:    path,
		w:       snappy.NewBufferedWriter(file),
		r:       snappy.NewReader(file),
		lenBuff: make([]byte, 8),
	}
}

type snapshotFile struct {
	*os.File
	r       *snappy.Reader
	w       *snappy.Writer
	lenBuff []byte
	path    string
}

func (s *snapshotFile) Path() string {
	return s.path
}

func (s *snapshotFile) Read(p []byte) (n int, err error) {
	buf := s.lenBuff[:]
	if _, err := io.ReadFull(s.r, buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint64(buf)
	if _, err := io.ReadFull(s.r, p[:size]); err != nil {
		return 0, err
	}
	return int(size), nil
}

func (s *snapshotFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf := s.lenBuff[:]
	binary.LittleEndian.PutUint64(buf, uint64(len(p)))
	_, err := s.w.Write(buf)
	if err != nil {
		return 0, err
	}

	n, err := s.w.Write(p)
	if err != nil {
		return 0, err
	}
	return n, err
}

func (s *snapshotFile) Sync() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.File.Sync(); err != nil {
		return err
	}
	return nil
}

func (s *snapshotFile) Close() error {
	if err := s.w.Close(); err != nil {
		return err
	}
	if err := s.File.Close(); err != nil {
		return err
	}
	return nil
}
