package regattaserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/snapshot"
	serrors "github.com/wandera/regatta/storage/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMaxGRPCSize is the default maximum size of body of gRPC message to be loaded from dragonboat.
	DefaultMaxGRPCSize = 4 * 1024 * 1024
	// DefaultSnapshotChunkSize default chunk size of gRPC snapshot stream.
	DefaultSnapshotChunkSize = 1024 * 1024
	// DefaultMaxLogRecords is a maximum number of log records sent via a single RPC call.
	DefaultMaxLogRecords = 10_000
	// DefaultCacheSize is a size of the cache used during the replication routine.
	DefaultCacheSize = 1024
)

// MetadataServer implements Metadata service from proto/replication.proto.
type MetadataServer struct {
	proto.UnimplementedMetadataServer
	Tables TableService
}

func (m *MetadataServer) Get(context.Context, *proto.MetadataRequest) (*proto.MetadataResponse, error) {
	tabs, err := m.Tables.GetTables()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "unknown err %v", err)
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

func (s *SnapshotServer) Stream(req *proto.SnapshotRequest, srv proto.Snapshot_StreamServer) error {
	table, err := s.Tables.GetTable(string(req.Table))
	if err != nil {
		return status.Errorf(codes.Unavailable, "unable to stream from table '%s': %v", req.GetTable(), err)
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
		_ = sf.Close()
		_ = os.Remove(sf.Path())
	}()

	resp, err := table.Snapshot(ctx, sf)
	if err != nil {
		return err
	}
	// Write dummy command with leader index to commit recovery snapshot.
	final, err := (&proto.Command{
		Table:       req.Table,
		Type:        proto.Command_DUMMY,
		LeaderIndex: &resp.Index,
	}).MarshalVT()
	if err != nil {
		return err
	}
	_, err = sf.Write(final)
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

// LogServer implements Log service from proto/replication.proto.
type LogServer struct {
	Tables    TableService
	LogReader LogReaderService
	Log       *zap.SugaredLogger

	maxMessageSize uint64
	metrics        struct {
		replicationIndex *prometheus.GaugeVec
	}
	proto.UnimplementedLogServer
}

func NewLogServer(ts TableService, lr LogReaderService, logger *zap.Logger, maxMessageSize uint64) *LogServer {
	ls := &LogServer{
		Tables:         ts,
		Log:            logger.Sugar().Named("log-replication-server"),
		LogReader:      lr,
		maxMessageSize: maxMessageSize,
		metrics: struct {
			replicationIndex *prometheus.GaugeVec
		}{
			replicationIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			),
		},
	}

	if maxMessageSize == 0 {
		ls.maxMessageSize = DefaultMaxGRPCSize
	}
	return ls
}

// Collect leader's metrics.
func (l *LogServer) Collect(ch chan<- prometheus.Metric) {
	tt, err := l.Tables.GetTables()
	if err != nil {
		l.Log.Errorf("cannot read tables: %v", err)
		return
	}

	for _, t := range tt {
		table, err := l.Tables.GetTable(t.Name)
		if err != nil {
			l.Log.Errorf("cannot get '%s' as active table: %v", t.Name, err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		idx, err := table.LocalIndex(ctx)
		if err != nil {
			l.Log.Errorf("cannot get local index for table '%s': %v", table.Name, err)
			cancel()
			continue
		}
		l.metrics.replicationIndex.With(prometheus.Labels{"role": "leader", "table": table.Name}).Set(float64(idx.Index))
		cancel()
	}

	l.metrics.replicationIndex.Collect(ch)
}

// Describe leader's metrics.
func (l *LogServer) Describe(ch chan<- *prometheus.Desc) {
	l.metrics.replicationIndex.Describe(ch)
}

var (
	repErrUseSnapshot  = errorResponseFactory(proto.ReplicateError_USE_SNAPSHOT)
	repErrLeaderBehind = errorResponseFactory(proto.ReplicateError_LEADER_BEHIND)
)

// Replicate entries from the leader's log.
func (l *LogServer) Replicate(req *proto.ReplicateRequest, server proto.Log_ReplicateServer) error {
	t, err := l.Tables.GetTable(string(req.GetTable()))
	if err != nil {
		return status.Errorf(codes.Unavailable, "unable to replicate table '%s': %v", req.GetTable(), err)
	}

	if req.LeaderIndex == 0 {
		return status.Error(codes.InvalidArgument, "invalid leaderIndex: leaderIndex must be greater than 0")
	}

	logRange := dragonboat.LogRange{FirstIndex: req.LeaderIndex, LastIndex: req.LeaderIndex + DefaultMaxLogRecords}
	for {
		entries, err := l.LogReader.QueryRaftLog(server.Context(), t.ClusterID, logRange, l.maxMessageSize)
		if errors.Is(err, serrors.ErrLogBehind) {
			err = server.Send(repErrLeaderBehind)
		} else if errors.Is(err, serrors.ErrLogAhead) {
			err = server.Send(repErrUseSnapshot)
		}

		if err != nil {
			return status.FromContextError(err).Err()
		}

		read := uint64(len(entries))
		if read == 0 {
			return nil
		}

		// Transform entries into actual commands.
		commands := make([]*proto.ReplicateCommand, 0, len(entries))
		for _, e := range entries {
			if cmd, err := entryToCommand(e); err != nil {
				return err
			} else {
				commands = append(commands, &proto.ReplicateCommand{Command: cmd, LeaderIndex: e.Index})
			}
		}

		msg := &proto.ReplicateResponse{
			Response: &proto.ReplicateResponse_CommandsResponse{
				CommandsResponse: &proto.ReplicateCommandsResponse{
					Commands: commands,
				},
			},
		}

		if err := server.Send(msg); err != nil {
			return status.FromContextError(err).Err()
		}

		logRange.FirstIndex += read
		logRange.LastIndex = logRange.FirstIndex + DefaultMaxLogRecords
	}
}

// errorResponseFactory creates a ReplicateResponse error.
func errorResponseFactory(err proto.ReplicateError) *proto.ReplicateResponse {
	return &proto.ReplicateResponse{
		Response: &proto.ReplicateResponse_ErrorResponse{
			ErrorResponse: &proto.ReplicateErrResponse{
				Error: err,
			},
		},
	}
}

// entryToCommand converts the raftpb.Entry to equivalent proto.ReplicateCommand.
func entryToCommand(e raftpb.Entry) (*proto.Command, error) {
	cmd := &proto.Command{}
	if e.Type != raftpb.EncodedEntry {
		cmd.Type = proto.Command_DUMMY
	} else if err := cmd.UnmarshalVT(e.Cmd[1:]); err != nil {
		return nil, err
	}
	cmd.LeaderIndex = &e.Index
	return cmd, nil
}
