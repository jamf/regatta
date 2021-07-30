package replication

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
)

const snapshotFilenamePattern = "snapshot-*.bin"

// NewSnapshot returns a new instance of Snapshot replicator.
func NewSnapshot(client proto.SnapshotClient, manager *tables.Manager) *Snapshot {
	return &Snapshot{
		SnapshotClient: client,
		TableManager:   manager,
		Timeout:        1 * time.Hour,
		closer:         make(chan struct{}),
		log:            zap.S().Named("replication").Named("snapshot"),
	}
}

// Snapshot connects to the snapshot service and obtains Raw snapshot data.
type Snapshot struct {
	SnapshotClient proto.SnapshotClient
	TableManager   *tables.Manager
	Timeout        time.Duration
	closer         chan struct{}
	log            *zap.SugaredLogger
}

func (s *Snapshot) Recover(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	stream, err := s.SnapshotClient.Stream(ctx, &proto.SnapshotRequest{Table: []byte(name)})
	if err != nil {
		return err
	}

	sf, err := newSnapshotFile(os.TempDir(), snapshotFilenamePattern)
	if err != nil {
		return err
	}
	defer func() {
		err := sf.Close()
		if err != nil {
			return
		}
		_ = os.Remove(sf.Path())
	}()

	reader := snapshotReader{stream: stream}
	buffer := make([]byte, 4*1024*1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		_, err = sf.Write(buffer[:n])
		if err != nil {
			return err
		}
	}
	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return s.TableManager.LoadTableFromSnapshot(name, sf)
}

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
