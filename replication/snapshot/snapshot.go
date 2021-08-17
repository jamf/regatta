package snapshot

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
	"github.com/wandera/regatta/proto"
)

const snapshotFilenamePattern = "snapshot-*.bin"

type Writer struct {
	Sender proto.Snapshot_StreamServer
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
	Stream proto.Snapshot_StreamClient
	Bucket *ratelimit.Bucket
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
	if s.Bucket != nil {
		s.Bucket.Wait(int64(chunk.Len))
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
		if s.Bucket != nil {
			s.Bucket.Wait(int64(chunk.Len))
		}
		w, err := w.Write(chunk.Data)
		if err != nil {
			return n, err
		}
		n = n + int64(w)
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
	return newFile(f, filepath.Join(dir, f.Name())), nil
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
	buf := s.lenBuff[:]
	binary.LittleEndian.PutUint64(buf, uint64(len(p)))
	n, err := s.w.Write(buf)
	if err != nil {
		return 0, err
	}

	m, err := s.w.Write(p)
	if err != nil {
		return 0, err
	}
	return n + m, err
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
