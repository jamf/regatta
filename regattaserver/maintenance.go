package regattaserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/snapshot"
)

// MaintenanceServer implements Maintenance service from proto/regatta.proto.
type MaintenanceServer struct {
	proto.UnimplementedMaintenanceServer
	Tables TableService
}

func (m *MaintenanceServer) Backup(req *proto.BackupRequest, srv proto.Maintenance_BackupServer) error {
	table, err := m.Tables.GetTable(string(req.Table))
	if err != nil {
		return err
	}

	ctx := srv.Context()
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(srv.Context(), 1*time.Hour)
		defer cancel()
		ctx = dctx
	}

	sf, err := snapshot.NewTemp()
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(sf.Path())
	}()

	_, err = table.Snapshot(ctx, sf)
	if err != nil {
		return err
	}
	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(&snapshot.Writer{Sender: srv}, bufio.NewReaderSize(sf.File, DefaultSnapshotChunkSize))
	return err
}

func (m *MaintenanceServer) Restore(srv proto.Maintenance_RestoreServer) error {
	msg, err := srv.Recv()
	if err != nil {
		return err
	}
	info := msg.GetInfo()
	if info == nil {
		return fmt.Errorf("first message should contain info")
	}
	sf, err := snapshot.NewTemp()
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(sf.Path())
	}()
	_, err = io.Copy(sf, restoreReader{stream: srv})
	if err != nil {
		return err
	}
	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	return m.Tables.Restore(string(info.Table), sf)
}

type restoreReader struct {
	stream proto.Maintenance_RestoreServer
}

func (s restoreReader) Read(p []byte) (int, error) {
	m, err := s.stream.Recv()
	if err != nil {
		return 0, err
	}
	chunk := m.GetChunk()
	if chunk == nil {
		return 0, errors.New("chunk expected")
	}
	if len(p) < int(chunk.Len) {
		return 0, io.ErrShortBuffer
	}
	return copy(p, chunk.Data), nil
}

func (s restoreReader) WriteTo(w io.Writer) (int64, error) {
	n := int64(0)
	for {
		m, err := s.stream.Recv()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		chunk := m.GetChunk()
		if chunk == nil {
			return 0, errors.New("chunk expected")
		}
		w, err := w.Write(chunk.Data)
		if err != nil {
			return n, err
		}
		n = n + int64(w)
	}
}
