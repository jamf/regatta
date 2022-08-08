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
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
)

const defaultQueryTimeout = 5 * time.Second

func New(cfg Config) (*Engine, error) {
	nh, err := createNodeHost(cfg)
	if err != nil {
		return nil, err
	}
	tm, err := createTableManager(cfg, nh)
	if err != nil {
		return nil, err
	}
	return &Engine{
		NodeHost: nh,
		Manager:  tm,
	}, nil
}

type Engine struct {
	*dragonboat.NodeHost
	*tables.Manager
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

func createTableManager(cfg Config, nh *dragonboat.NodeHost) (*tables.Manager, error) {
	tm := tables.NewManager(nh, cfg.InitialMembers,
		tables.Config{
			NodeID: cfg.NodeID,
			Table:  tables.TableConfig(cfg.Table),
			Meta:   tables.MetaConfig(cfg.Meta),
		})
	return tm, nil
}

func createNodeHost(cfg Config) (*dragonboat.NodeHost, error) {
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
