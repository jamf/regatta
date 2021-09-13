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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResetServer implements some Maintenance service methods from proto/regatta.proto.
type ResetServer struct {
	proto.UnimplementedMaintenanceServer
	Tables TableService
}

func (m *ResetServer) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	reset := func(name string) error {
		t, err := m.Tables.GetTable(name)
		if err != nil {
			return err
		}
		return t.Reset(ctx)
	}
	if req.ResetAll {
		tables, err := m.Tables.GetTables()
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			err := reset(table.Name)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	if len(req.Table) <= 0 {
		return nil, status.Error(codes.InvalidArgument, "table name must not be empty")
	}
	err := reset(string(req.Table))
	if err != nil {
		return nil, err
	}
	return &proto.ResetResponse{}, nil
}

// BackupServer implements some Maintenance service methods from proto/regatta.proto.
type BackupServer struct {
	proto.UnimplementedMaintenanceServer
	Tables TableService
}

func (m *BackupServer) Backup(req *proto.BackupRequest, srv proto.Maintenance_BackupServer) error {
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

func (m *BackupServer) Restore(srv proto.Maintenance_RestoreServer) error {
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
	_, err = io.Copy(sf.File, backupReader{stream: srv})
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
	err = m.Tables.Restore(string(info.Table), sf)
	if err != nil {
		return err
	}
	return srv.SendAndClose(&proto.RestoreResponse{})
}

type backupReader struct {
	stream proto.Maintenance_RestoreServer
}

func (s backupReader) Read(p []byte) (int, error) {
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

func (s backupReader) WriteTo(w io.Writer) (int64, error) {
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
