package replication

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
	"github.com/wandera/regatta/proto"
)

const snapshotFilenamePattern = "snapshot-*.bin"

type snapshotReader struct {
	stream proto.Snapshot_StreamClient
}

func (s snapshotReader) Read(p []byte) (n int, err error) {
	chunk, err := s.stream.Recv()
	if err != nil {
		return 0, err
	}
	if len(p) < int(chunk.Len) {
		return 0, io.ErrShortBuffer
	}
	return copy(p, chunk.Data), nil
}

func newSnapshotFile(dir, pattern string) (*snapshotFile, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return &snapshotFile{
		File:    f,
		path:    filepath.Join(dir, f.Name()),
		w:       snappy.NewBufferedWriter(f),
		r:       snappy.NewReader(f),
		lenBuff: make([]byte, 8),
	}, nil
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
