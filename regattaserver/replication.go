package regattaserver

import (
	"context"
	"time"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetadataServer implements Metadata service from proto/replication.proto.
type MetadataServer struct {
	proto.UnimplementedMetadataServer
	Tables TableService
}

func (m *MetadataServer) Get(context.Context, *proto.MetadataRequest) (*proto.MetadataResponse, error) {
	tabs, err := m.Tables.GetTables()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unknown err %v", err)
	}
	resp := &proto.MetadataResponse{}
	for _, tab := range tabs {
		resp.Tables = append(resp.Tables, &proto.Table{
			Type: proto.Table_REPLICATED,
			Name: tab.Name,
		})
	}
	return resp, nil
}

// SnapshotServer implements Snapshot service from proto/replication.proto.
type SnapshotServer struct {
	proto.UnimplementedSnapshotServer
	Tables TableService
}

type snapshotWriter struct {
	sender proto.Snapshot_StreamServer
}

func (g *snapshotWriter) Write(p []byte) (int, error) {
	ln := len(p)
	if err := g.sender.Send(&proto.SnapshotChunk{
		Data: p,
		Len:  uint64(ln),
	}); err != nil {
		return 0, err
	}
	return ln, nil
}

func (s *SnapshotServer) Stream(req *proto.SnapshotRequest, srv proto.Snapshot_StreamServer) error {
	table, err := s.Tables.GetTable(string(req.Table))
	if err != nil {
		return err
	}

	ctx := srv.Context()
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(srv.Context(), 1*time.Hour)
		defer cancel()
		ctx = dctx
	}

	return table.Snapshot(ctx, &snapshotWriter{sender: srv})
}

// LogServer implements Log service from proto/replication.proto.
type LogServer struct {
	proto.UnimplementedLogServer
}

func (l *LogServer) Replicate(*proto.ReplicateRequest, proto.Log_ReplicateServer) error {
	return nil
}
