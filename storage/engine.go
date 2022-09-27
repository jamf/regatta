package storage

import (
	"context"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	dbl "github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/plugin/tan"
	"github.com/lni/dragonboat/v4/raftio"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/logreader"
	"github.com/wandera/regatta/storage/tables"
)

const defaultQueryTimeout = 5 * time.Second

// nodeShardToInfoIndex indexes `replicaID` -> `shardID` -> `raftio.LeaderInfo`.
type nodeShardToInfoIndex map[uint64]map[uint64]raftio.LeaderInfo

func (i nodeShardToInfoIndex) clone() nodeShardToInfoIndex {
	c := nodeShardToInfoIndex{}
	for replicaID, m := range i {
		c[replicaID] = map[uint64]raftio.LeaderInfo{}
		for shardID, info := range m {
			c[replicaID][shardID] = info
		}
	}
	return c
}

// shardToLeaderIndex indexes `shardID` -> `replicaID` of a leader node.
type shardToLeaderIndex map[uint64]uint64

func newClusterView() *clusterView {
	return &clusterView{
		infoIndex:   nodeShardToInfoIndex{},
		leaderIndex: shardToLeaderIndex{},
	}
}

type clusterView struct {
	infoIndex   nodeShardToInfoIndex
	leaderIndex shardToLeaderIndex
	lock        sync.RWMutex
}

func (v *clusterView) update(info raftio.LeaderInfo) {
	v.lock.Lock()
	defer v.lock.Unlock()

	if info.LeaderID == 0 {
		previousLeader := v.leaderIndex[info.ShardID]
		delete(v.leaderIndex, info.ShardID)
		v.mutateNodeMap(previousLeader, func(m map[uint64]raftio.LeaderInfo) {
			delete(m, info.ShardID)
		})
	} else {
		v.leaderIndex[info.ShardID] = info.LeaderID
		v.mutateNodeMap(info.LeaderID, func(m map[uint64]raftio.LeaderInfo) {
			m[info.ShardID] = info
		})
	}
}

func (v *clusterView) snapshot() clusterViewSnapshot {
	v.lock.RLock()
	defer v.lock.RUnlock()
	return clusterViewSnapshot(v.infoIndex.clone())
}

func (v *clusterView) mutateNodeMap(nodeID uint64, f func(m map[uint64]raftio.LeaderInfo)) {
	m, ok := v.infoIndex[nodeID]
	if !ok {
		m = make(map[uint64]raftio.LeaderInfo, 0)
	}

	f(m)
	v.infoIndex[nodeID] = m

	if len(m) <= 0 {
		delete(v.infoIndex, nodeID)
	}
}

type clusterViewSnapshot map[uint64]map[uint64]raftio.LeaderInfo

func (s clusterViewSnapshot) info(nodeID uint64, shardID uint64) raftio.LeaderInfo {
	if shards, ok := s[nodeID]; ok {
		if info, ok := shards[shardID]; ok {
			return info
		}
	}
	return raftio.LeaderInfo{}
}

func New(cfg Config) (*Engine, error) {
	e := &Engine{
		cfg:         cfg,
		clusterView: newClusterView(),
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
	e.Manager = manager
	e.LogReader = lr
	return e, nil
}

type Engine struct {
	*dragonboat.NodeHost
	*tables.Manager
	cfg         Config
	LogReader   *logreader.LogReader
	clusterView *clusterView
}

func (e *Engine) Start() error {
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
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	rng, err := table.Range(ctx, req)
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
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	put, err := table.Put(ctx, req)
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
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	del, err := table.Delete(ctx, req)
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
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	tx, err := table.Txn(ctx, req)
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
	info := e.clusterView.snapshot().info(e.NodeID(), shardID)
	header.ShardId = info.ShardID
	header.ReplicaId = info.ReplicaID
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

func (e *Engine) LeaderUpdated(info raftio.LeaderInfo) {
	e.clusterView.update(info)
}

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
func (e *Engine) LogCompacted(info raftio.EntryInfo)               {}
func (e *Engine) LogDBCompacted(info raftio.EntryInfo)             {}

func createNodeHost(cfg Config, sel raftio.ISystemEventListener, rel raftio.IRaftEventListener) (*dragonboat.NodeHost, error) {
	dbl.SetLoggerFactory(rl.LoggerFactory(cfg.Logger))
	dbl.GetLogger("raft").SetLevel(dbl.INFO)
	dbl.GetLogger("rsm").SetLevel(dbl.WARNING)
	dbl.GetLogger("transport").SetLevel(dbl.WARNING)
	dbl.GetLogger("dragonboat").SetLevel(dbl.WARNING)
	dbl.GetLogger("logdb").SetLevel(dbl.INFO)
	dbl.GetLogger("settings").SetLevel(dbl.INFO)

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
	} else {
		nhc.Expert.LogDB = buildLogDBConfig()
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
