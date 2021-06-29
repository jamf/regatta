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

func (m *Manager) CreateTable(name string) error {
	created, err := m.createTable(name)
	if err != nil {
		return err
	}
	return m.startTable(created)
}

func (m *Manager) createTable(name string) (table.Table, error) {
	storeName := storedTableName(name)
	if m.store.Exists(storeName) {
		return table.Table{}, ErrTableExists
	}
	seq, err := m.incAndGetIDSeq()
	if err != nil {
		return table.Table{}, err
	}
	tab := table.Table{
		Name:      name,
		ClusterID: seq,
	}
	bytes, err := json.Marshal(&tab)
	if err != nil {
		return table.Table{}, err
	}
	_, err = m.store.Set(storeName, string(bytes), 0)
	if err != nil {
		if err == kv.ErrVersionMismatch {
			return table.Table{}, ErrTableExists
		}
		return table.Table{}, err
	}
	return tab, nil
}

func (m *Manager) DeleteTable(name string) error {
	storeName := storedTableName(name)
	tab, err := m.store.Get(storeName)
	if err != nil {
		return err
	}
	return m.store.Delete(storeName, tab.Ver)
}

func storedTableName(name string) string {
	return keyPrefix + name
}

func (m *Manager) GetTable(name string) (table.ActiveTable, error) {
	v, err := m.store.Get(storedTableName(name))
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
	return tab.AsActive(m.nh), nil
}

func (m *Manager) Start() error {
	go func() {
		for {
			_, ok, _ := m.nh.GetLeaderID(metaFSMClusterID)
			if ok {
				go m.reconcileLoop()
				close(m.readyChan)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return m.nh.StartConcurrentCluster(m.members, false, kv.NewLFSM(), metaRaftConfig(m.cfg.NodeID, m.cfg.Meta))
}

func (m *Manager) WaitUntilReady() error {
	for {
		select {
		case <-m.readyChan:
			return nil
		case <-m.closed:
			return ErrManagerClosed
		}
	}
}

func (m *Manager) Close() {
	close(m.closed)
}

func (m *Manager) reconcileLoop() {
	for {
		select {
		case <-m.closed:
			return
		default:
			err := m.reconcile()
			if err != nil {
				m.log.Errorf("reconcile failed: %v", err)
			}
			time.Sleep(m.reconcileInterval)
		}
	}
}

func (m *Manager) reconcile() error {
	tabs, err := m.getTables()
	if err != nil {
		return err
	}
	start, stop := diffTables(tabs, m.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption).ClusterInfoList)
	for _, tab := range start {
		err = m.startTable(tab)
		if err != nil {
			return err
		}
	}
	for _, tab := range stop {
		err = m.nh.StopCluster(tab)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) incAndGetIDSeq() (uint64, error) {
	seq, err := m.store.Get(sequenceKey)
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

	_, err = m.store.Set(seq.Key, strconv.FormatUint(next, 10), seq.Ver)
	return next, err
}

func (m *Manager) getTables() (map[string]table.Table, error) {
	tables := make(map[string]table.Table)
	all, err := m.store.GetAll(keyPrefix + "*")
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

func diffTables(tables map[string]table.Table, raftInfo []dragonboat.ClusterInfo) (toStart []table.Table, toStop []uint64) {
	tableIDs := make(map[uint64]table.Table)
	for _, t := range tables {
		tableIDs[t.ClusterID] = t
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

func (m *Manager) startTable(tbl table.Table) error {
	return m.nh.StartOnDiskCluster(
		m.members,
		false,
		table.NewFSM(tbl.Name, m.cfg.Table.NodeHostDir, m.cfg.Table.WALDir, m.cfg.Table.FS),
		tableRaftConfig(m.cfg.NodeID, tbl.ClusterID, m.cfg.Table),
	)
}

func tableRaftConfig(nodeID, clusterID uint64, cfg TableConfig) config.Config {
	return config.Config{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		CheckQuorum:             true,
		ElectionRTT:             cfg.ElectionRTT,
		HeartbeatRTT:            cfg.HeartbeatRTT,
		SnapshotEntries:         cfg.SnapshotEntries,
		CompactionOverhead:      cfg.CompactionOverhead,
		OrderedConfigChange:     true,
		MaxInMemLogSize:         cfg.MaxInMemLogSize,
		SnapshotCompressionType: config.Snappy,
	}
}

func metaRaftConfig(nodeID uint64, cfg MetaConfig) config.Config {
	return config.Config{
		NodeID:              nodeID,
		ClusterID:           metaFSMClusterID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         cfg.ElectionRTT,
		HeartbeatRTT:        cfg.HeartbeatRTT,
		SnapshotEntries:     cfg.SnapshotEntries,
		CompactionOverhead:  cfg.CompactionOverhead,
		OrderedConfigChange: true,
		MaxInMemLogSize:     cfg.MaxInMemLogSize,
	}
}
