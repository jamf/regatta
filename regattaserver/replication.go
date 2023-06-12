// Copyright JAMF Software, LLC

package regattaserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"time"

	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/replication/snapshot"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMaxGRPCSize is the default maximum size of body of gRPC message to be loaded from dragonboat.
	DefaultMaxGRPCSize = 4 * 1024 * 1024
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

	_, err = io.Copy(&snapshot.Writer{Sender: srv}, bufio.NewReaderSize(sf.File, snapshot.DefaultSnapshotChunkSize))
	return err
}

// LogServer implements Log service from proto/replication.proto.
type LogServer struct {
	Tables    TableService
	LogReader LogReaderService
	Log       *zap.SugaredLogger

	maxMessageSize uint64
	proto.UnimplementedLogServer
}

func NewLogServer(ts TableService, lr LogReaderService, logger *zap.Logger, maxMessageSize uint64) *LogServer {
	ls := &LogServer{
		Tables:         ts,
		Log:            logger.Sugar().Named("log-replication-server"),
		LogReader:      lr,
		maxMessageSize: maxMessageSize,
	}

	if maxMessageSize == 0 {
		ls.maxMessageSize = DefaultMaxGRPCSize
	}
	return ls
}

var (
	repErrUseSnapshot  = errorResponseFactory(proto.ReplicateError_USE_SNAPSHOT)
	repErrLeaderBehind = errorResponseFactory(proto.ReplicateError_LEADER_BEHIND)
)

// Replicate entries from the leader's log.
func (l *LogServer) Replicate(req *proto.ReplicateRequest, server proto.Log_ReplicateServer) error {
	if req.LeaderIndex == 0 {
		return status.Error(codes.InvalidArgument, "invalid leaderIndex: leaderIndex must be greater than 0")
	}

	t, err := l.Tables.GetTable(string(req.GetTable()))
	if err != nil {
		return status.Errorf(codes.Unavailable, "unable to replicate table '%s': %v", req.GetTable(), err)
	}

	appliedIndex, err := t.LocalIndex(server.Context())
	if err != nil {
		return status.FromContextError(err).Err()
	}

	if appliedIndex.Index+1 < req.LeaderIndex {
		return status.FromContextError(server.Send(repErrLeaderBehind)).Err()
	}

	logRange := dragonboat.LogRange{FirstIndex: req.LeaderIndex, LastIndex: appliedIndex.Index + 1}
	for {
		entries, err := l.LogReader.QueryRaftLog(server.Context(), t.ClusterID, logRange, l.maxMessageSize)
		switch {
		case errors.Is(err, serrors.ErrLogBehind):
			return status.FromContextError(server.Send(repErrLeaderBehind)).Err()
		case errors.Is(err, serrors.ErrLogAhead):
			return status.FromContextError(server.Send(repErrUseSnapshot)).Err()
		case err != nil:
			return status.FromContextError(err).Err()
		default:
		}

		read := uint64(len(entries))
		if read == 0 {
			if err := server.Send(&proto.ReplicateResponse{LeaderIndex: appliedIndex.Index}); err != nil {
				return status.FromContextError(err).Err()
			}
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
			LeaderIndex: appliedIndex.Index,
			Response: &proto.ReplicateResponse_CommandsResponse{
				CommandsResponse: &proto.ReplicateCommandsResponse{
					Commands: commands,
				},
			},
		}

		if err := server.Send(msg); err != nil {
			return status.FromContextError(err).Err()
		}
		next := entries[len(entries)-1].Index + 1
		logRange.FirstIndex = uint64(math.Min(float64(next), float64(logRange.LastIndex)))
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
