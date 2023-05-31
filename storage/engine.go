// Copyright JAMF Software, LLC

package storage

import (
	"context"
	"time"

	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/storage/cluster"
	"github.com/jamf/regatta/storage/logreader"
	"github.com/jamf/regatta/storage/tables"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/plugin/tan"
	"github.com/lni/dragonboat/v4/raftio"
	protobuf "google.golang.org/protobuf/proto"
)

const defaultQueryTimeout = 5 * time.Second

func New(cfg Config) (*Engine, error) {
	e := &Engine{
		cfg: cfg,
	}
	nh, err := createNodeHost(cfg, e, e)
	if err != nil {
		return nil, err
	}

	manager := tables.NewManager(
		nh,
		cfg.InitialMembers,
		tables.Config{
			NodeID: cfg.NodeID,
			Table:  tables.TableConfig(cfg.Table),
			Meta:   tables.MetaConfig(cfg.Meta),
		},
	)
	lr := &logreader.LogReader{
		ShardCacheSize: cfg.LogCacheSize,
		LogQuerier:     nh,
	}
	e.NodeHost = nh

	clst, err := cluster.New(cfg.Gossip.BindAddress, cfg.Gossip.AdvertiseAddress, e.clusterInfo)
	if err != nil {
		return nil, err
	}
	e.Cluster = clst
	e.Manager = manager
	e.LogReader = lr
	return e, nil
}

type Engine struct {
	*dragonboat.NodeHost
	*tables.Manager
	cfg       Config
	LogReader *logreader.LogReader
	Cluster   *cluster.Cluster
}

func (e *Engine) Start() error {
	_, err := e.Cluster.Start(e.cfg.Gossip.InitialMembers)
	if err != nil {
		return err
	}
	return e.Manager.Start()
}

func (e *Engine) Close() error {
	e.Manager.Close()
	e.NodeHost.Close()
	return nil
}

func (e *Engine) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	table, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	rng, err := withDefaultTimeout(ctx, req, table.Range)
	if err != nil {
		return nil, err
	}
	rng.Header = e.getHeader(nil, table.ClusterID)
	return rng, nil
}

func (e *Engine) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	table, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	put, err := withDefaultTimeout(ctx, req, table.Put)
	if err != nil {
		return nil, err
	}
	put.Header = e.getHeader(put.Header, table.ClusterID)
	return put, nil
}

func (e *Engine) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	table, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	del, err := withDefaultTimeout(ctx, req, table.Delete)
	if err != nil {
		return nil, err
	}
	del.Header = e.getHeader(del.Header, table.ClusterID)
	return del, nil
}

func (e *Engine) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	table, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	tx, err := withDefaultTimeout(ctx, req, table.Txn)
	if err != nil {
		return nil, err
	}
	tx.Header = e.getHeader(tx.Header, table.ClusterID)
	return tx, nil
}

func (e *Engine) getHeader(header *proto.ResponseHeader, shardID uint64) *proto.ResponseHeader {
	if header == nil {
		header = &proto.ResponseHeader{}
	}
	header.ReplicaId = e.cfg.NodeID
	header.ShardId = shardID
	info := e.Cluster.ShardInfo(shardID)
	header.RaftTerm = info.Term
	header.RaftLeaderId = info.LeaderID
	return header
}

func (e *Engine) NodeDeleted(info raftio.NodeInfo) {
	if info.ReplicaID == e.NodeID() {
		e.LogReader.NodeDeleted(info)
	}
}

func (e *Engine) NodeReady(info raftio.NodeInfo) {
	if info.ReplicaID == e.NodeID() {
		e.LogReader.NodeReady(info)
	}
}

func (e *Engine) LeaderUpdated(info raftio.LeaderInfo)             {}
func (e *Engine) NodeHostShuttingDown()                            {}
func (e *Engine) NodeUnloaded(info raftio.NodeInfo)                {}
func (e *Engine) MembershipChanged(info raftio.NodeInfo)           {}
func (e *Engine) ConnectionEstablished(info raftio.ConnectionInfo) {}
func (e *Engine) ConnectionFailed(info raftio.ConnectionInfo)      {}
func (e *Engine) SendSnapshotStarted(info raftio.SnapshotInfo)     {}
func (e *Engine) SendSnapshotCompleted(info raftio.SnapshotInfo)   {}
func (e *Engine) SendSnapshotAborted(info raftio.SnapshotInfo)     {}
func (e *Engine) SnapshotReceived(info raftio.SnapshotInfo)        {}
func (e *Engine) SnapshotRecovered(info raftio.SnapshotInfo)       {}
func (e *Engine) SnapshotCreated(info raftio.SnapshotInfo)         {}
func (e *Engine) SnapshotCompacted(info raftio.SnapshotInfo)       {}
func (e *Engine) LogCompacted(info raftio.EntryInfo) {
	if info.ReplicaID == e.NodeID() {
		e.LogReader.LogCompacted(info)
	}
}
func (e *Engine) LogDBCompacted(info raftio.EntryInfo) {}

func (e *Engine) clusterInfo() cluster.Info {
	nhi := e.NodeHost.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
	return cluster.Info{
		NodeHostID:    e.NodeHost.ID(),
		NodeID:        e.cfg.NodeID,
		RaftAddress:   e.cfg.RaftAddress,
		ShardInfoList: nhi.ShardInfoList,
		LogInfo:       nhi.LogInfo,
	}
}

func createNodeHost(cfg Config, sel raftio.ISystemEventListener, rel raftio.IRaftEventListener) (*dragonboat.NodeHost, error) {
	nhc := config.NodeHostConfig{
		WALDir:              cfg.WALDir,
		NodeHostDir:         cfg.NodeHostDir,
		RTTMillisecond:      cfg.RTTMillisecond,
		RaftAddress:         cfg.RaftAddress,
		ListenAddress:       cfg.ListenAddress,
		EnableMetrics:       true,
		MaxReceiveQueueSize: cfg.MaxReceiveQueueSize,
		MaxSendQueueSize:    cfg.MaxSendQueueSize,
		SystemEventListener: sel,
		RaftEventListener:   rel,
	}

	if cfg.LogDBImplementation == Tan {
		nhc.Expert.LogDBFactory = tan.Factory
	}
	nhc.Expert.LogDB = buildLogDBConfig()

	if cfg.FS != nil {
		nhc.Expert.FS = cfg.FS
	}

	err := nhc.Prepare()
	if err != nil {
		return nil, err
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	return nh, nil
}

func buildLogDBConfig() config.LogDBConfig {
	cfg := config.GetSmallMemLogDBConfig()
	cfg.KVRecycleLogFileNum = 4
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	return cfg
}

func withDefaultTimeout[R protobuf.Message, S protobuf.Message](ctx context.Context, req R, f func(context.Context, R) (S, error)) (S, error) {
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	return f(ctx, req)
}
