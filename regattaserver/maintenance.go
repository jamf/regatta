package regattaserver

import (
	"bufio"
	"context"
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
