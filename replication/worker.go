package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

var (
	ErrLeaderBehind = errors.New("leader behind")
	ErrUseSnapshot  = errors.New("use snapshot")
)

type workerFactory struct {
	interval       time.Duration
	timeout        time.Duration
	tm             *tables.Manager
	log            *zap.SugaredLogger
	nh             *dragonboat.NodeHost
	logClient      proto.LogClient
	snapshotClient proto.SnapshotClient
}

func (f workerFactory) Create(table string) *worker {
	return &worker{
		logClient:      f.logClient,
		snapshotClient: f.snapshotClient,
		tm:             f.tm,
		nh:             f.nh,
		interval:       f.interval,
		timeout:        f.timeout,
		Table:          table,
		closer:         make(chan struct{}),
		log:            f.log.Named(table),
	}
}

// worker connects to the log replication service and synchronizes the local state.
type worker struct {
	interval       time.Duration
	timeout        time.Duration
	tm             *tables.Manager
	Table          string
	closer         chan struct{}
	log            *zap.SugaredLogger
	nh             *dragonboat.NodeHost
	logClient      proto.LogClient
	snapshotClient proto.SnapshotClient
}

// Start launches the replication goroutine. To stop it, call worker.Close.
func (l *worker) Start() {
	go func() {
		l.log.Info("log replication started")
		t := time.NewTicker(l.interval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if !l.tm.IsLeader() {
					l.log.Debug("skipping replication - not a leader")
					continue
				}
				if err := l.do(); err != nil {
					if err == ErrLeaderBehind {
						l.log.Errorf("the leader log is behind the replication will stop")
						return
					}
					l.log.Warnf("log replication error %v", err)
				}
			case <-l.closer:
				l.log.Info("log replication stopped")
				return
			}
		}
	}()
}

func (l worker) do() error {
	t, err := l.tm.GetTable(l.Table)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()
	idxRes, err := t.LeaderIndex(ctx)
	if err != nil {
		return fmt.Errorf("could not get leader index key: %w", err)
	}

	replicateRequest := &proto.ReplicateRequest{
		LeaderIndex: idxRes.Index + 1,
		Table:       []byte(l.Table),
	}

	stream, err := l.logClient.Replicate(ctx, replicateRequest)
	if err != nil {
		return fmt.Errorf("could not open log stream: %w", err)
	}

	if err = l.read(ctx, stream, t.ClusterID); err != nil {
		switch err {
		case ErrUseSnapshot:
			return l.recover(ctx, t.Name, 3*time.Hour)
		case ErrLeaderBehind:
			return ErrLeaderBehind
		default:
			return fmt.Errorf("could not store the log stream: %w", err)
		}
	}
	return nil
}

// read commands from the stream and save them to the cluster.
func (l *worker) read(ctx context.Context, stream proto.Log_ReplicateClient, clusterID uint64) error {
	for {
		replicateRes, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not read from the stream: %v", err)
		}

		switch res := replicateRes.Response.(type) {
		case *proto.ReplicateResponse_CommandsResponse:
			if err := l.proposeBatch(ctx, res.CommandsResponse.GetCommands(), clusterID); err != nil {
				return fmt.Errorf("could not propose: %w", err)
			}
		case *proto.ReplicateResponse_ErrorResponse:
			if res.ErrorResponse.Error == proto.ReplicateError_LEADER_BEHIND {
				return ErrLeaderBehind
			}

			if res.ErrorResponse.Error == proto.ReplicateError_USE_SNAPSHOT {
				return ErrUseSnapshot
			}

			return fmt.Errorf(
				"unknown replicate error response '%s' with id %d",
				res.ErrorResponse.Error.String(),
				res.ErrorResponse.Error,
			)
		}
	}
}

// Close stops the log replication.
func (l *worker) Close() { l.closer <- struct{}{} }

func (l *worker) proposeBatch(ctx context.Context, commands []*proto.ReplicateCommand, clusterID uint64) error {
	for _, c := range commands {
		bytes, err := pb.Marshal(c.Command)
		if err != nil {
			return fmt.Errorf("could not marshal command: %w", err)
		}

		if _, err := l.nh.SyncPropose(ctx, l.nh.GetNoOPSession(clusterID), bytes); err != nil {
			return fmt.Errorf("could not SyncPropose: %w", err)
		}
	}
	return nil
}

func (l *worker) recover(ctx context.Context, name string, timeout time.Duration) error {
	l.log.Info("recovering from snapshot")
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := l.snapshotClient.Stream(ctx, &proto.SnapshotRequest{Table: []byte(name)})
	if err != nil {
		return err
	}

	sf, err := newSnapshotFile(os.TempDir(), snapshotFilenamePattern)
	if err != nil {
		return err
	}
	defer func() {
		err := sf.Close()
		if err != nil {
			return
		}
		_ = os.Remove(sf.Path())
	}()

	reader := snapshotReader{stream: stream}
	buffer := make([]byte, 4*1024*1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		_, err = sf.Write(buffer[:n])
		if err != nil {
			return err
		}
	}
	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	l.log.Info("snapshot stream saved, loading table")
	err = l.tm.LoadTableFromSnapshot(name, sf)
	if err != nil {
		return err
	}
	l.log.Info("table recovered")
	return nil
}