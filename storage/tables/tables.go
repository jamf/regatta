package tables

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/wandera/regatta/storage/kv"
	"github.com/wandera/regatta/storage/table"
	"go.uber.org/zap"
)

type store interface {
	Exists(key string) bool
	Set(key string, value string, ver uint64) (kv.Pair, error)
	Delete(key string, ver uint64) error
	Get(key string) (kv.Pair, error)
	GetAll(pattern string) (kv.Pairs, error)
}

const (
	keyPrefix                 = "/tables/"
	sequenceKey               = keyPrefix + "sys/idseq"
	metaFSMClusterID          = 1000
	tableIDsRangeStart uint64 = 10000
)

var (
	ErrTableExists       = errors.New("table already exists")
	ErrTableDoesNotExist = errors.New("table does not exist")
	ErrManagerClosed     = errors.New("manager closed")
)

func NewManager(nh *dragonboat.NodeHost, members map[uint64]string, cfg Config) *Manager {
	return &Manager{
		nh:                nh,
		reconcileInterval: 30 * time.Second,
		readyChan:         make(chan struct{}),
		members:           members,
		cfg:               cfg,
		store: &kv.RaftStore{
			NodeHost:  nh,
			ClusterID: metaFSMClusterID,
		},
		closed: make(chan struct{}),
		log:    zap.S().Named("manager"),
	}
}

type Manager struct {
	store             store
	nh                *dragonboat.NodeHost
	members           map[uint64]string
	closed            chan struct{}
	cfg               Config
	readyChan         chan struct{}
	reconcileInterval time.Duration
	log               *zap.SugaredLogger
}

func (t *Manager) CreateTable(name string) error {
	err := t.createTable(name)
	if err != nil {
		return err
	}
	return t.startTable(name)
}

func (t *Manager) createTable(name string) error {
	storeName := keyPrefix + name
	if t.store.Exists(storeName) {
		return ErrTableExists
	}
	seq, err := t.incAndGetIDSeq()
	if err != nil {
		return err
	}
	tab := table.Table{
		Name:      name,
		ClusterID: seq,
	}
	bytes, err := json.Marshal(&tab)
	if err != nil {
		return err
	}
	_, err = t.store.Set(storeName, string(bytes), 0)
	if err != nil {
		if err == kv.ErrVersionMismatch {
			return ErrTableExists
		}
		return err
	}
	return nil
}

func (t *Manager) DeleteTable(name string) error {
	storedName := keyPrefix + name
	tab, err := t.store.Get(storedName)
	if err != nil {
		return err
	}
	return t.store.Delete(storedName, tab.Ver)
}

func (t *Manager) GetTable(name string) (table.ActiveTable, error) {
	v, err := t.store.Get(keyPrefix + name)
	if err != nil {
		if err == kv.ErrNotExist {
			return table.ActiveTable{}, ErrTableDoesNotExist
		}
		return table.ActiveTable{}, err
	}
	tab := table.Table{}
	err = json.Unmarshal([]byte(v.Value), &tab)
	if err != nil {
		return table.ActiveTable{}, err
	}
	return tab.AsActive(t.nh), nil
}

func (t *Manager) Start() error {
	go func() {
		for {
			_, ok, _ := t.nh.GetLeaderID(metaFSMClusterID)
			if ok {
				go t.reconcileLoop()
				close(t.readyChan)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return t.nh.StartConcurrentCluster(t.members, false, kv.NewLFSM(), metaRaftConfig(t.cfg.NodeID, t.cfg.Meta))
}

func (t *Manager) WaitUntilReady() error {
	for {
		select {
		case <-t.readyChan:
			return nil
		case <-t.closed:
			return ErrManagerClosed
		}
	}
}

func (t *Manager) Close() {
	close(t.closed)
}

func (t *Manager) reconcileLoop() {
	for {
		select {
		case <-t.closed:
			return
		default:
			time.Sleep(t.reconcileInterval)
			err := t.reconcile()
			if err != nil {
				t.log.Errorf("reconcile failed: %v", err)
			}
		}
	}
}

func (t *Manager) reconcile() error {
	tabs, err := t.getTables()
	if err != nil {
		return err
	}
	start, stop := diffTables(tabs, t.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption).ClusterInfoList)
	for _, tab := range start {
		err = t.startTable(tab)
		if err != nil {
			return err
		}
	}
	for _, tab := range stop {
		err = t.nh.StopCluster(tab)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Manager) incAndGetIDSeq() (uint64, error) {
	seq, err := t.store.Get(sequenceKey)
	if err != nil {
		if err == kv.ErrNotExist {
			seq = kv.Pair{
				Key:   sequenceKey,
				Value: strconv.FormatUint(tableIDsRangeStart, 10),
				Ver:   0,
			}
		} else {
			return 0, err
		}
	}
	currSeq, err := strconv.ParseUint(seq.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	next := currSeq + 1

	_, err = t.store.Set(seq.Key, strconv.FormatUint(next, 10), seq.Ver)
	return next, err
}

func (t *Manager) getTables() (map[string]table.Table, error) {
	tables := make(map[string]table.Table)
	all, err := t.store.GetAll(keyPrefix + "*")
	if err != nil {
		return nil, err
	}
	for _, v := range all {
		tab := table.Table{}
		err = json.Unmarshal([]byte(v.Value), &tab)
		if err != nil {
			return nil, err
		}
		tables[tab.Name] = tab
	}

	return tables, nil
}

func diffTables(tables map[string]table.Table, raftInfo []dragonboat.ClusterInfo) (toStart []string, toStop []uint64) {
	tableIDs := make(map[uint64]string)
	for _, t := range tables {
		tableIDs[t.ClusterID] = t.Name
	}
	raftTableIDs := make(map[uint64]struct{})
	for _, t := range raftInfo {
		raftTableIDs[t.ClusterID] = struct{}{}
	}

	for tID, tName := range tableIDs {
		_, found := raftTableIDs[tID]
		if !found && tID > tableIDsRangeStart {
			toStart = append(toStart, tName)
		}
	}

	for rID := range raftTableIDs {
		_, found := tableIDs[rID]
		if !found && rID > tableIDsRangeStart {
			toStop = append(toStop, rID)
		}
	}
	return
}

func (t *Manager) startTable(name string) error {
	get, err := t.store.Get(keyPrefix + name)
	if err != nil {
		return err
	}
	tab := table.Table{}
	err = json.Unmarshal([]byte(get.Value), &tab)
	if err != nil {
		return err
	}
	return t.nh.StartOnDiskCluster(
		t.members,
		false,
		table.New(name, t.cfg.Table.NodeHostDir, t.cfg.Table.WALDir, t.cfg.Table.FS),
		tableRaftConfig(t.cfg.NodeID, tab.ClusterID, t.cfg.Table),
	)
}

func tableRaftConfig(nodeID, clusterID uint64, tcfg Table) config.Config {
	return config.Config{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		CheckQuorum:             true,
		ElectionRTT:             tcfg.ElectionRTT,
		HeartbeatRTT:            tcfg.HeartbeatRTT,
		SnapshotEntries:         tcfg.SnapshotEntries,
		CompactionOverhead:      tcfg.CompactionOverhead,
		OrderedConfigChange:     true,
		MaxInMemLogSize:         tcfg.MaxInMemLogSize,
		SnapshotCompressionType: config.Snappy,
	}
}

func metaRaftConfig(nodeID uint64, mcfg Meta) config.Config {
	return config.Config{
		NodeID:              nodeID,
		ClusterID:           metaFSMClusterID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         mcfg.ElectionRTT,
		HeartbeatRTT:        mcfg.HeartbeatRTT,
		SnapshotEntries:     mcfg.SnapshotEntries,
		CompactionOverhead:  mcfg.CompactionOverhead,
		OrderedConfigChange: true,
		MaxInMemLogSize:     mcfg.MaxInMemLogSize,
	}
}
