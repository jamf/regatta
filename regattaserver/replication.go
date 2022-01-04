package regattaserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/snapshot"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMaxGRPCSize is the default maximum size of body of gRPC message to be loaded from dragonboat.
	DefaultMaxGRPCSize = 4 * 1024 * 1024
	// DefaultSnapshotChunkSize default chunk size of gRPC snapshot stream.
	DefaultSnapshotChunkSize = 1024 * 1024
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
	DB             raftio.ILogDB
	Tables         TableService
	NodeID         uint64
	Log            *zap.SugaredLogger
	maxMessageSize uint64

	metrics struct {
		replicationIndex *prometheus.GaugeVec
	}
	proto.UnimplementedLogServer
}

func NewLogServer(tm *tables.Manager, db raftio.ILogDB, logger *zap.Logger, maxMessageSize uint64) *LogServer {
	ls := &LogServer{
		Tables:         tm,
		DB:             db,
		NodeID:         tm.NodeID(),
		Log:            logger.Sugar().Named("log-replication-server"),
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
		return fmt.Errorf("no table '%s' found: %v", req.GetTable(), err)
	}

	if req.LeaderIndex == 0 {
		return fmt.Errorf("invalid leaderIndex: leaderIndex must be greater than 0")
	}

	rs, leaderBehind, err := l.readRaftState(t.ClusterID, req.LeaderIndex)
	if err != nil {
		return err
	}
	if leaderBehind {
		return server.Send(repErrLeaderBehind)
	}

	lastIndex := rs.FirstIndex + rs.EntryCount

	// Follower is up-to-date with the leader, therefore there are no new data to be sent.
	if lastIndex == req.LeaderIndex {
		return nil
	}

	// Follower's leaderIndex is in the leader's snapshot, not in the log.
	if req.LeaderIndex < rs.FirstIndex {
		return server.Send(repErrUseSnapshot)
	}

	// Follower is behind and all entries can be sent from the leader's log.
	ctx := server.Context()
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(server.Context(), 1*time.Minute)
		defer cancel()
		ctx = dctx
	}
	return l.readLog(ctx, server, t.ClusterID, req.LeaderIndex, lastIndex)
}

func (l *LogServer) readRaftState(clusterID, leaderIndex uint64) (rs raftio.RaftState, leaderBehind bool, err error) {
	defer func() {
		if errRec := recover(); errRec != nil {
			if msg, ok := errRec.(string); ok && strings.HasPrefix(msg, "first index") {
				// ReadRaftState panicked because req.LeaderIndex > rs.MaxIndex.
				// When the leader is behind, ReadRaftState causes panic with
				// a string that starts with "first index".
				leaderBehind = true
				l.Log.Warnf("leader behind - %s (leaderIndex %d)", msg, leaderIndex)
			} else {
				// Something else caused the panic, so raise as error.
				err = fmt.Errorf("ReadRaftState panicked: %v", errRec)
			}
		}
	}()

	rs, err = l.DB.ReadRaftState(clusterID, l.NodeID, leaderIndex-1)
	if err != nil {
		err = fmt.Errorf("could not get raft state: %v", err)
		return
	}
	return
}

func (l *LogServer) iterateEntries(entries []raftpb.Entry, clusterID, lo, hi uint64) (ents []raftpb.Entry, err error) {
	defer func() {
		if errRec := recover(); errRec != nil {
			err = fmt.Errorf("IterateEntries panicked: %v", errRec)
		}
	}()
	ents, _, err = l.DB.IterateEntries(entries, math.MaxUint64, clusterID, l.NodeID, lo, hi, l.maxMessageSize)
	return
}

// readLog and write it to the stream.
func (l *LogServer) readLog(ctx context.Context, server proto.Log_ReplicateServer, clusterID, firstIndex, lastIndex uint64) error {
	var (
		commands []*proto.ReplicateCommand
		entries  []raftpb.Entry
	)

	for lo, hi := firstIndex, lastIndex; lo < hi; {
		select {
		case <-ctx.Done():
			l.Log.Info("ending replication stream, deadline reached")
			return nil
		default:
		}

		// TODO make interval configurable
		if d, ok := ctx.Deadline(); ok && time.Until(d) < 1*time.Second {
			l.Log.Info("ending replication stream, deadline soon to be reached")
			return nil
		}

		commands = commands[:0]
		entries = entries[:0]

		entries, err := l.iterateEntries(entries, clusterID, lo, hi)
		if err != nil {
			return fmt.Errorf("could not iterate over entries: %v", err)
		}

		if len(entries) == 0 {
			return nil
		}

		for _, e := range entries {
			cmd, err := entryToCommand(e)
			if err != nil {
				return err
			}

			if cmd != nil {
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

		if err = server.Send(msg); err != nil {
			return err
		}

		lo += uint64(len(entries))
	}

	return nil
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
