package replication

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

// NewLog returns a new instance of Log replicator for a given table.
func NewLog(client proto.LogClient, manager *tables.Manager, nh *dragonboat.NodeHost, table string, interval time.Duration) *Log {
	return &Log{
		LogClient:    client,
		TableManager: manager,
		Interval:     interval,
		Timeout:      5 * time.Second,
		Table:        table,
		closer:       make(chan struct{}),
		log:          zap.S().Named("replication").Named("log"),
		nh:           nh,
	}
}

// Log connects to the log replication service and synchronizes the local state.
type Log struct {
	LogClient    proto.LogClient
	TableManager *tables.Manager
	Interval     time.Duration
	Timeout      time.Duration
	Table        string
	closer       chan struct{}
	log          *zap.SugaredLogger
	nh           *dragonboat.NodeHost
}

// Replicate launches the log replication goroutine. To stop it, call Log.Close.
func (l *Log) Replicate() {
	go func() {
		l.log.Info("log replication started")
		t := time.NewTicker(l.Interval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if !l.TableManager.IsLeader() {
					continue
				}

				t, err := l.TableManager.GetTable(l.Table)
				if err != nil {
					l.log.Errorf("could not find table '%s': %v", l.Table, err)
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), l.Timeout)
				idxRes, err := t.LeaderIndex(ctx)
				if err != nil {
					cancel()
					l.log.Errorf("could not get leader index key: %v", err)
					continue
				}

				replicateRequest := &proto.ReplicateRequest{
					LeaderIndex: idxRes.Index + 1,
					Table:       []byte(l.Table),
				}

				stream, err := l.LogClient.Replicate(ctx, replicateRequest)
				if err != nil {
					cancel()
					l.log.Errorf("log replicate request failed: %v", err)
					continue
				}

				if err = l.read(ctx, stream, t.ClusterID); err != nil {
					l.log.Warnf("could not replicate the log: %v", err)
				}
				cancel()
			case <-l.closer:
				l.log.Info("log replication stopped")
				return
			}
		}
	}()
}

// read commands from the stream and save them to the cluster.
func (l *Log) read(ctx context.Context, stream proto.Log_ReplicateClient, clusterID uint64) error {
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
			if err := l.proposeBatch(res.CommandsResponse.GetCommands(), clusterID); err != nil {
				return fmt.Errorf("could not propose: %v", err)
			}
		case *proto.ReplicateResponse_ErrorResponse:
			if res.ErrorResponse.Error == proto.ReplicateError_LEADER_BEHIND {
				return fmt.Errorf("leader behind")
			}

			if res.ErrorResponse.Error == proto.ReplicateError_USE_SNAPSHOT {
				// TODO: Call Coufy's Snapshot.Recover.
				return nil
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
func (l *Log) Close() { close(l.closer) }

func (l *Log) proposeBatch(cc []*proto.ReplicateCommand, clusterID uint64) error {
	propose := func(bytes []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), l.Timeout)
		defer cancel()
		if _, err := l.nh.SyncPropose(ctx, l.nh.GetNoOPSession(clusterID), bytes); err != nil {
			return err
		}
		return nil
	}

	for _, c := range cc {
		bytes, err := pb.Marshal(c.Command)
		if err != nil {
			return fmt.Errorf("could not marshal command: %v", err)
		}

		if err = propose(bytes); err != nil {
			return fmt.Errorf("could not SyncPropose: %v", err)
		}
	}
	return nil
}
