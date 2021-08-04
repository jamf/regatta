package replication

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

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
	return &snapshotFile{File: f, path: filepath.Join(dir, f.Name())}, nil
}

type snapshotFile struct {
	*os.File
	path string
}

func (s *snapshotFile) Path() string {
	return s.path
}

func (s *snapshotFile) Read(p []byte) (n int, err error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(s.File, buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint64(buf)
	if _, err := io.ReadFull(s.File, p[:size]); err != nil {
		return 0, err
	}
	return int(size), nil
}

func (s *snapshotFile) Write(p []byte) (int, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(p)))

	n, err := s.File.Write(buf)
	if err != nil {
		return 0, err
	}

	m, err := s.File.Write(p)
	if err != nil {
		return 0, err
	}
	return n + m, err
}
