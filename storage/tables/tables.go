package tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/kv"
	"github.com/wandera/regatta/storage/table"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
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
		cache: struct {
			mu     sync.RWMutex
			tables map[string]table.ActiveTable
		}{
			tables: make(map[string]table.ActiveTable),
		},
		closed: make(chan struct{}),
		log:    zap.S().Named("manager"),
	}
}

type Manager struct {
	store store
	nh    *dragonboat.NodeHost
	cache struct {
		mu     sync.RWMutex
		tables map[string]table.ActiveTable
	}
	members           map[uint64]string
	closed            chan struct{}
	cfg               Config
	readyChan         chan struct{}
	reconcileInterval time.Duration
	log               *zap.SugaredLogger
}

type Lease struct {
	ID    uint64    `json:"id"`
	Until time.Time `json:"until"`
}

func (m *Manager) LeaseTable(name string, lease time.Duration) error {
	key := storedTableName(name) + "/lease"
	get, err := m.store.Get(key)

	unclaimed := err == kv.ErrNotExist
	if err != nil && !unclaimed {
		return err
	}

	l := Lease{}
	if !unclaimed {
		err = json.Unmarshal([]byte(get.Value), &l)
		if err != nil {
			return err
		}
	}

	if unclaimed || l.ID == m.cfg.NodeID || l.Until.Before(time.Now()) {
		l.ID = m.cfg.NodeID
		l.Until = time.Now().Add(lease)
	}

	bts, err := json.Marshal(l)
	if err != nil {
		return err
	}
	_, err = m.store.Set(key, string(bts), get.Ver)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) CreateTable(name string) error {
	created, err := m.createTable(name)
	if err != nil {
		return err
	}
	return m.startTable(created.Name, created.ClusterID)
}

func (m *Manager) IsLeader() bool {
	id, b, _ := m.nh.GetLeaderID(metaFSMClusterID)
	return b && id == m.cfg.NodeID
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
	err = m.setTableVersion(tab, 0)
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
	return fmt.Sprintf("%s%s", keyPrefix, name)
}

func (m *Manager) GetTable(name string) (table.ActiveTable, error) {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()
	if t, ok := m.cache.tables[name]; ok {
		return t, nil
	}

	tab, _, err := m.getTableVersion(name)
	if err != nil {
		return table.ActiveTable{}, err
	}
	return tab.AsActive(m.nh), nil
}

func (m *Manager) GetTables() ([]table.Table, error) {
	tabs, err := m.getTables()
	if err != nil {
		return nil, err
	}
	rtabs := make([]table.Table, 0, len(tabs))
	for _, t := range tabs {
		rtabs = append(rtabs, t)
	}
	return rtabs, nil
}

func (m *Manager) Start() error {
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-m.closed:
				return
			case <-t.C:
				_, ok, _ := m.nh.GetLeaderID(metaFSMClusterID)
				if ok {
					go m.reconcileLoop()
					close(m.readyChan)
					return
				}
			}
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

func (m *Manager) NodeID() uint64 {
	return m.cfg.NodeID
}

func (m *Manager) reconcileLoop() {
	t := time.NewTicker(m.reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-m.closed:
			return
		case <-t.C:
			err := m.reconcile()
			if err != nil {
				m.log.Errorf("reconcile failed: %v", err)
			}
		}
	}
}

func (m *Manager) reconcile() error {
	tabs, err := m.getTables()
	if err != nil {
		return err
	}
	for _, t := range tabs {
		m.cacheTable(t)
	}

	start, stop := diffTables(tabs, m.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption).ClusterInfoList)
	for id, tbl := range start {
		err = m.startTable(tbl.Name, id)
		if err != nil {
			return err
		}
		m.cacheTable(tbl)
	}

	for _, tab := range stop {
		err = m.stopTable(tab)
		if err != nil {
			return err
		}
		m.clearTable(tab)
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

func diffTables(tables map[string]table.Table, raftInfo []dragonboat.ClusterInfo) (toStart map[uint64]table.Table, toStop []uint64) {
	tableIDs := make(map[uint64]table.Table)
	for _, t := range tables {
		if t.ClusterID != 0 {
			tableIDs[t.ClusterID] = t
		}
		if t.RecoverID != 0 {
			tableIDs[t.RecoverID] = t
		}
	}
	raftTableIDs := make(map[uint64]struct{})
	for _, t := range raftInfo {
		raftTableIDs[t.ClusterID] = struct{}{}
	}

	for tID, tName := range tableIDs {
		_, found := raftTableIDs[tID]
		if !found && tID > tableIDsRangeStart {
			if toStart == nil {
				toStart = make(map[uint64]table.Table)
			}
			toStart[tID] = tName
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

func (m *Manager) startTable(name string, id uint64) error {
	return m.nh.StartOnDiskCluster(
		m.members,
		false,
		table.NewFSM(name, m.cfg.Table.NodeHostDir, m.cfg.Table.WALDir, m.cfg.Table.FS),
		tableRaftConfig(m.cfg.NodeID, id, m.cfg.Table),
	)
}

func (m *Manager) cacheTable(tbl table.Table) {
	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()
	m.cache.tables[tbl.Name] = tbl.AsActive(m.nh)
}

func (m *Manager) clearTable(clusterID uint64) {
	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()
	for name, activeTable := range m.cache.tables {
		if activeTable.ClusterID == clusterID {
			delete(m.cache.tables, name)
		}
	}
}

func (m *Manager) stopTable(clusterID uint64) error {
	err := m.nh.StopCluster(clusterID)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) LoadTableFromSnapshot(name string, reader io.Reader) error {
	tbl, version, err := m.getTableVersion(name)
	if err != nil && err != ErrTableDoesNotExist {
		return err
	}
	recoveryID, err := m.incAndGetIDSeq()
	if err != nil {
		return err
	}

	tbl.Name = name
	tbl.RecoverID = recoveryID

	err = m.startTable(tbl.Name, tbl.RecoverID)
	if err != nil {
		return err
	}

	err = m.setTableVersion(tbl, version)
	if err != nil {
		return err
	}

	err = m.waitForLeader(tbl.RecoverID)
	if err != nil {
		return err
	}

	err = m.readIntoTable(tbl.RecoverID, reader)
	if err != nil {
		return err
	}

	tbl, version, err = m.getTableVersion(name)
	if err != nil {
		return err
	}

	tbl.ClusterID = tbl.RecoverID
	tbl.RecoverID = 0
	err = m.setTableVersion(tbl, version)
	if err != nil {
		return err
	}
	m.cacheTable(tbl)
	return nil
}

func (m *Manager) getTableVersion(name string) (table.Table, uint64, error) {
	v, err := m.store.Get(storedTableName(name))
	if err != nil {
		if err == kv.ErrNotExist {
			return table.Table{}, 0, ErrTableDoesNotExist
		}
		return table.Table{}, 0, err
	}
	tab := table.Table{}
	err = json.Unmarshal([]byte(v.Value), &tab)
	return tab, v.Ver, err
}

func (m *Manager) setTableVersion(tbl table.Table, version uint64) error {
	storeName := storedTableName(tbl.Name)
	bts, err := json.Marshal(&tbl)
	if err != nil {
		return err
	}
	_, err = m.store.Set(storeName, string(bts), version)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) readIntoTable(id uint64, reader io.Reader) error {
	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = 0
	session := m.nh.GetNoOPSession(id)
	msg := make([]byte, 1024*1024*4)

	batchCmd := proto.Command{
		Type: proto.Command_PUT_BATCH,
	}
	last := false
	for {
		n, err := reader.Read(msg)
		if err != nil {
			if err == io.EOF {
				last = true
			} else {
				return err
			}
		}

		if !last {
			cmd := &proto.Command{}
			err = pb.Unmarshal(msg[:n], cmd)
			if err != nil {
				return err
			}

			batchCmd.Table = cmd.Table
			batchCmd.LeaderIndex = cmd.LeaderIndex

			if uint64(pb.Size(&batchCmd)+pb.Size(cmd)) < m.cfg.Table.MaxInMemLogSize {
				batchCmd.Batch = append(batchCmd.Batch, cmd.Kv)
				continue
			}
		}

		bb, err := pb.Marshal(&batchCmd)
		if err != nil {
			return err
		}
		batchCmd = proto.Command{
			Type: proto.Command_PUT_BATCH,
		}

		err = backoff.Retry(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := m.nh.SyncPropose(ctx, session, bb)
			return err
		}, backOff)

		if err != nil {
			return err
		}

		if last {
			break
		}
	}
	return nil
}

func (m *Manager) waitForLeader(clusterID uint64) error {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	// TODO make configurable
	ctx, cancel := context.WithTimeout(context.Background(), m.reconcileInterval*2)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, ok, _ := m.nh.GetLeaderID(clusterID)
			if ok {
				return nil
			}
		}
	}
}

func tableRaftConfig(nodeID, clusterID uint64, cfg TableConfig) config.Config {
	return config.Config{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		CheckQuorum:             true,
		OrderedConfigChange:     true,
		ElectionRTT:             cfg.ElectionRTT,
		HeartbeatRTT:            cfg.HeartbeatRTT,
		SnapshotEntries:         cfg.SnapshotEntries,
		CompactionOverhead:      cfg.CompactionOverhead,
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
