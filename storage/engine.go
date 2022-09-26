package storage

import (
	"context"
	"os"
	"path"
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

func New(cfg Config) (*Engine, error) {
	e := &Engine{cfg: cfg}
	nh, err := createNodeHost(cfg, e)
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
		NodeHost:       nh,
	}

	e.NodeHost = nh
	e.Manager = manager
	e.LogReader = lr
	return e, nil
}

type Engine struct {
	*dragonboat.NodeHost
	*tables.Manager
	cfg       Config
	LogReader *logreader.LogReader
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
	return table.Range(ctx, req)
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
	return table.Put(ctx, req)
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
	return table.Delete(ctx, req)
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
	return table.Txn(ctx, req)
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

func createNodeHost(cfg Config, sel raftio.ISystemEventListener) (*dragonboat.NodeHost, error) {
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

	fixNHID(nhc.NodeHostDir)

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	return nh, nil
}

// TODO Remove after release.
func fixNHID(dir string) {
	idPath := path.Join(dir, "NODEHOST.ID")
	bytes, _ := os.ReadFile(idPath)
	if len(bytes) != 0 && len(bytes) < 24 {
		_ = os.Remove(idPath)
	}
}

func buildLogDBConfig() config.LogDBConfig {
	cfg := config.GetSmallMemLogDBConfig()
	cfg.KVRecycleLogFileNum = 4
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	return cfg
}
