package regattaserver

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	protobuf "google.golang.org/protobuf/proto"
)

const (
	// MaxGRPCSize is the maximum size of body of gRPC message, which is 4MiB - 1KiB.
	MaxGRPCSize = (4 * 1024 * 1024) - 1024
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
	DB     raftio.ILogDB
	Tables TableService
	NodeID uint64
	Log    *zap.SugaredLogger

	proto.UnimplementedLogServer
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
	return l.readLog(server, t.ClusterID, rs.FirstIndex+1, lastIndex)
}

func (l *LogServer) readRaftState(clusterID, leaderIndex uint64) (rs raftio.RaftState, leaderBehind bool, err error) {
	defer func() {
		if errRec := recover(); errRec != nil {
			if msg, ok := errRec.(string); ok && strings.HasPrefix(msg, "first index") {
				// ReadRaftState panicked because req.LeaderIndex > rs.MaxIndex.
				// When the leader is behind, ReadRaftState causes panic with
				// a string that starts with "first index".
				leaderBehind = true
				l.Log.Warnf("leader behind - %s", msg)
			} else {
				// Something else caused the panic, so re-raise it.
				panic(errRec)
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

// readLog and write it to the stream.
func (l *LogServer) readLog(server proto.Log_ReplicateServer, clusterID, firstIndex, lastIndex uint64) error {
	var (
		commands []*proto.ReplicateCommand
		entries  []raftpb.Entry
	)

	for lo, hi := firstIndex, lastIndex; lo < hi; {
		commands = commands[:0]
		entries = entries[:0]

		entries, _, err := l.DB.IterateEntries(entries, math.MaxUint64, clusterID, l.NodeID, lo, hi, MaxGRPCSize)
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
	if e.Type != raftpb.EncodedEntry {
		return nil, nil
	}

	cmd := &proto.Command{}
	err := protobuf.Unmarshal(e.Cmd[1:], cmd)
	return cmd, err
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
