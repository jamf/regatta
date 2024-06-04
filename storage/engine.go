// Copyright JAMF Software, LLC

package storage

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/config"
	"github.com/jamf/regatta/raft/plugin/tan"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/cluster"
	"github.com/jamf/regatta/storage/kv"
	"github.com/jamf/regatta/storage/logreader"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/util/iter"
	"github.com/jamf/regatta/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultQueryTimeout = 5 * time.Second
	tableStoreID        = 1000
)

func New(cfg Config) (*Engine, error) {
	e := &Engine{
		cfg:  cfg,
		log:  cfg.Log,
		stop: make(chan struct{}),
	}
	e.events = &events{eventsCh: make(chan any, 1), stopc: make(chan struct{}), engine: e}

	nh, err := createNodeHost(e)
	if err != nil {
		return nil, fmt.Errorf("failed to start raft nodehost: %w", err)
	}
	e.NodeHost = nh

	name := cfg.Gossip.NodeName
	if name == "" {
		name = nh.ID()
	}
	clst, err := cluster.New(cfg.Gossip.BindAddress, cfg.Gossip.AdvertiseAddress, cfg.Gossip.ClusterName, name, e.clusterInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap gossip cluster: %w", err)
	}
	e.Cluster = clst
	e.tableStore = &kv.RaftStore{
		NodeHost:  nh,
		ClusterID: tableStoreID,
	}
	e.Manager = table.NewManager(
		nh,
		cfg.InitialMembers,
		e.tableStore,
		table.Config{
			NodeID: cfg.NodeID,
			Table:  table.TableConfig(cfg.Table),
			Meta:   table.MetaConfig(cfg.Meta),
		},
	)
	if cfg.LogCacheSize > 0 {
		e.LogCache = logreader.NewShardCache(cfg.LogCacheSize)
		e.LogReader = &logreader.Cached{LogQuerier: nh, ShardCache: e.LogCache}
	} else {
		e.LogReader = &logreader.Simple{LogQuerier: nh}
	}
	return e, nil
}

type Engine struct {
	*raft.NodeHost
	*table.Manager
	cfg        Config
	log        *zap.SugaredLogger
	events     *events
	stop       chan struct{}
	LogReader  logreader.Interface
	Cluster    *cluster.Cluster
	LogCache   *logreader.ShardCache
	tableStore *kv.RaftStore
}

func (e *Engine) Start() error {
	e.Cluster.Start(e.cfg.Gossip.InitialMembers)
	if err := e.tableStore.Start(kv.RaftConfig{
		NodeID:             e.cfg.NodeID,
		ElectionRTT:        e.cfg.Meta.ElectionRTT,
		HeartbeatRTT:       e.cfg.Meta.HeartbeatRTT,
		SnapshotEntries:    e.cfg.Meta.SnapshotEntries,
		CompactionOverhead: e.cfg.Meta.CompactionOverhead,
		MaxInMemLogSize:    e.cfg.Meta.MaxInMemLogSize,
		InitialMembers:     e.cfg.InitialMembers,
	}); err != nil {
		return err
	}
	e.Manager.Start()
	go e.events.dispatchEvents()
	return nil
}

func (e *Engine) Close() error {
	close(e.stop)
	e.Manager.Close()
	e.NodeHost.Close()
	return nil
}

func (e *Engine) Range(ctx context.Context, req *regattapb.RangeRequest) (*regattapb.RangeResponse, error) {
	t, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	rng, err := withDefaultTimeout(ctx, req, t.Range)
	if err != nil {
		return nil, err
	}
	rng.Header = e.getHeader(nil, t.ClusterID)
	return rng, nil
}

func (e *Engine) IterateRange(ctx context.Context, req *regattapb.RangeRequest) (iter.Seq[*regattapb.RangeResponse], error) {
	t, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	it, err := withDefaultTimeout(ctx, req, t.Iterator)
	if err != nil {
		return nil, err
	}
	return iter.Map(it, func(s *regattapb.ResponseOp_Range) *regattapb.RangeResponse {
		return &regattapb.RangeResponse{
			Header: e.getHeader(nil, t.ClusterID),
			Kvs:    s.Kvs,
			More:   s.More,
			Count:  s.Count,
		}
	}), nil
}

func (e *Engine) Put(ctx context.Context, req *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	t, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	put, err := withDefaultTimeout(ctx, req, t.Put)
	if err != nil {
		return nil, err
	}
	put.Header = e.getHeader(put.Header, t.ClusterID)
	return put, nil
}

func (e *Engine) Delete(ctx context.Context, req *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	t, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	del, err := withDefaultTimeout(ctx, req, t.Delete)
	if err != nil {
		return nil, err
	}
	del.Header = e.getHeader(del.Header, t.ClusterID)
	return del, nil
}

func (e *Engine) Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	t, err := e.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	tx, err := withDefaultTimeout(ctx, req, t.Txn)
	if err != nil {
		return nil, err
	}
	tx.Header = e.getHeader(tx.Header, t.ClusterID)
	return tx, nil
}

func (e *Engine) MemberList(ctx context.Context, r *regattapb.MemberListRequest) (*regattapb.MemberListResponse, error) {
	return withDefaultTimeout(ctx, r, func(ctx context.Context, r *regattapb.MemberListRequest) (*regattapb.MemberListResponse, error) {
		nodes := e.Cluster.Nodes()
		res := &regattapb.MemberListResponse{Cluster: e.Cluster.Name(), Members: make([]*regattapb.Member, len(nodes))}
		for i, node := range nodes {
			res.Members[i] = &regattapb.Member{
				Id:         strconv.FormatUint(node.NodeID, 10),
				Name:       node.Name,
				PeerURLs:   []string{node.RaftAddress},
				ClientURLs: []string{node.ClientAddress},
			}
		}
		return res, nil
	})
}

func (e *Engine) Status(ctx context.Context, r *regattapb.StatusRequest) (*regattapb.StatusResponse, error) {
	return withDefaultTimeout(ctx, r, func(ctx context.Context, _ *regattapb.StatusRequest) (*regattapb.StatusResponse, error) {
		res := &regattapb.StatusResponse{
			Id:      strconv.FormatUint(e.cfg.NodeID, 10),
			Version: version.Version,
			Tables:  make(map[string]*regattapb.TableStatus),
		}
		tables, err := e.GetTables()
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
		}
		for _, t := range tables {
			at, err := e.GetTable(t.Name)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", t.Name, err.Error()))
				continue
			}
			index, err := at.LocalIndex(ctx, false)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", t.Name, err.Error()))
				continue
			}
			lid, term, _, err := e.GetLeaderID(at.ClusterID)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", t.Name, err.Error()))
				continue
			}
			res.Tables[at.Name] = &regattapb.TableStatus{
				Leader:           strconv.FormatUint(lid, 10),
				RaftIndex:        index.Index,
				RaftTerm:         term,
				RaftAppliedIndex: index.Index,
			}
		}
		return res, nil
	})
}

func (e *Engine) getHeader(header *regattapb.ResponseHeader, shardID uint64) *regattapb.ResponseHeader {
	if header == nil {
		header = &regattapb.ResponseHeader{}
	}
	header.ReplicaId = e.cfg.NodeID
	header.ShardId = shardID
	info := e.Cluster.ShardInfo(shardID)
	header.RaftTerm = info.Term
	header.RaftLeaderId = info.LeaderID
	return header
}

func (e *Engine) clusterInfo() cluster.Info {
	info := cluster.Info{
		NodeID:        e.cfg.NodeID,
		RaftAddress:   e.cfg.RaftAddress,
		ClientAddress: e.cfg.ClientAddress,
	}
	info.NodeHostID = e.NodeHost.ID()
	if nhi := e.NodeHost.GetNodeHostInfo(raft.DefaultNodeHostInfoOption); nhi != nil {
		info.ShardInfoList = nhi.ShardInfoList
		info.LogInfo = nhi.LogInfo
	}
	return info
}

func (e *Engine) WaitUntilReady(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return e.tableStore.WaitForLeader(ctx)
	})
	return eg.Wait()
}

func (e *Engine) Config() Config {
	return e.cfg
}

func createNodeHost(e *Engine) (*raft.NodeHost, error) {
	nhc := config.NodeHostConfig{
		WALDir:              e.cfg.WALDir,
		NodeHostDir:         e.cfg.NodeHostDir,
		RTTMillisecond:      e.cfg.RTTMillisecond,
		RaftAddress:         e.cfg.RaftAddress,
		ListenAddress:       e.cfg.ListenAddress,
		EnableMetrics:       true,
		MaxReceiveQueueSize: e.cfg.MaxReceiveQueueSize,
		MaxSendQueueSize:    e.cfg.MaxSendQueueSize,
		SystemEventListener: e.events,
		RaftEventListener:   e.events,
	}

	if e.cfg.LogDBImplementation == Tan {
		nhc.Expert.LogDBFactory = tan.Factory
	}
	nhc.Expert.LogDB = buildLogDBConfig()

	if e.cfg.FS != nil {
		nhc.Expert.FS = e.cfg.FS
	}

	err := nhc.Prepare()
	if err != nil {
		return nil, err
	}

	nh, err := raft.NewNodeHost(nhc)
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

func withDefaultTimeout[R any, S any](ctx context.Context, req R, f func(context.Context, R) (S, error)) (S, error) {
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	return f(ctx, req)
}
