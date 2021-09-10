package regattaserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/backup"
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
	_, err = io.Copy(sf.File, backup.Reader{Stream: srv})
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
