// Copyright JAMF Software, LLC

package regattaserver

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"time"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftpb"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/replication/snapshot"
	serrors "github.com/jamf/regatta/storage/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMaxGRPCSize is the default maximum size of body of gRPC message to be loaded from dragonboat.
	DefaultMaxGRPCSize = 4 * 1024 * 1024
)

// MetadataServer implements Metadata service from proto/replication.proto.
type MetadataServer struct {
	regattapb.UnimplementedMetadataServer
	Tables TableService
}

func (m *MetadataServer) Get(context.Context, *regattapb.MetadataRequest) (*regattapb.MetadataResponse, error) {
	tabs, err := m.Tables.GetTables()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "unknown err %v", err)
	}
	resp := &regattapb.MetadataResponse{}
	for _, tab := range tabs {
		resp.Tables = append(resp.Tables, &regattapb.Table{
			Type: regattapb.Table_REPLICATED,
			Name: tab.Name,
		})
		slices.SortFunc(resp.Tables, func(a, b *regattapb.Table) int {
			return cmp.Compare(a.Name, b.Name)
		})
	}
	return resp, nil
}

// SnapshotServer implements Snapshot service from proto/replication.proto.
type SnapshotServer struct {
	regattapb.UnimplementedSnapshotServer
	Tables TableService
}

func (s *SnapshotServer) Stream(req *regattapb.SnapshotRequest, srv regattapb.Snapshot_StreamServer) error {
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
	final, err := (&regattapb.Command{
		Table:       req.Table,
		Type:        regattapb.Command_DUMMY,
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
	regattapb.UnimplementedLogServer
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
	repErrUseSnapshot  = errorResponseFactory(regattapb.ReplicateError_USE_SNAPSHOT)
	repErrLeaderBehind = errorResponseFactory(regattapb.ReplicateError_LEADER_BEHIND)
)

// Replicate entries from the leader's log.
func (l *LogServer) Replicate(req *regattapb.ReplicateRequest, server regattapb.Log_ReplicateServer) error {
	if req.LeaderIndex == 0 {
		return status.Error(codes.InvalidArgument, "invalid leaderIndex: leaderIndex must be greater than 0")
	}

	t, err := l.Tables.GetTable(string(req.GetTable()))
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		if serrors.IsSafeToRetry(err) {
			return status.Error(codes.Unavailable, err.Error())
		}
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	ctx := server.Context()
	appliedIndex, err := t.LocalIndex(ctx, true)
	if err != nil {
		if serrors.IsSafeToRetry(err) {
			return status.Error(codes.Unavailable, err.Error())
		}
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	if appliedIndex.Index+1 < req.LeaderIndex {
		return server.Send(repErrLeaderBehind)
	}
	logRange := raft.LogRange{FirstIndex: req.LeaderIndex, LastIndex: appliedIndex.Index + 1}
	for {
		if dl, ok := ctx.Deadline(); ok && time.Now().After(dl) {
			l.Log.Infof("replication passed the deadline, ending stream prematurely")
			return nil
		}

		entries, err := l.LogReader.QueryRaftLog(ctx, t.ClusterID, logRange, l.maxMessageSize)
		switch {
		case errors.Is(err, serrors.ErrLogBehind):
			return server.Send(repErrLeaderBehind)
		case errors.Is(err, serrors.ErrLogAhead):
			return server.Send(repErrUseSnapshot)
		case err != nil:
			l.Log.Errorf("unknown error queriyng the raft log: %v", err)
			return nil
		default:
		}

		if len(entries) == 0 {
			// query index for update.
			appliedIndex, err := t.LocalIndex(ctx, false)
			if err != nil {
				return err
			}
			if err := server.Send(&regattapb.ReplicateResponse{LeaderIndex: appliedIndex.Index}); err != nil {
				return err
			}
			return nil
		}

		// Transform entries into actual commands.
		commands := make([]*regattapb.ReplicateCommand, 0, len(entries))
		for _, e := range entries {
			if cmd, err := entryToCommand(e); err != nil {
				return err
			} else {
				commands = append(commands, &regattapb.ReplicateCommand{Command: cmd, LeaderIndex: e.Index})
			}
		}

		msg := &regattapb.ReplicateResponse{
			LeaderIndex: appliedIndex.Index,
			Response: &regattapb.ReplicateResponse_CommandsResponse{
				CommandsResponse: &regattapb.ReplicateCommandsResponse{
					Commands: commands,
				},
			},
		}

		if err := server.Send(msg); err != nil {
			return err
		}
		next := entries[len(entries)-1].Index + 1
		logRange.FirstIndex = min(next, logRange.LastIndex)
	}
}

// errorResponseFactory creates a ReplicateResponse error.
func errorResponseFactory(err regattapb.ReplicateError) *regattapb.ReplicateResponse {
	return &regattapb.ReplicateResponse{
		Response: &regattapb.ReplicateResponse_ErrorResponse{
			ErrorResponse: &regattapb.ReplicateErrResponse{
				Error: err,
			},
		},
	}
}

// entryToCommand converts the raftpb.Entry to equivalent proto.ReplicateCommand.
func entryToCommand(e raftpb.Entry) (*regattapb.Command, error) {
	cmd := &regattapb.Command{}
	if e.Type != raftpb.EncodedEntry {
		cmd.Type = regattapb.Command_DUMMY
	} else if err := cmd.UnmarshalVT(e.Cmd[1:]); err != nil {
		return nil, err
	}
	cmd.LeaderIndex = &e.Index
	return cmd, nil
}
