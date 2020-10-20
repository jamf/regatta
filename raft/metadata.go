package raft

import (
	"sync"

	"github.com/lni/dragonboat/v3/raftio"
)

// Metadata stores Raft cluster metadata in time.
type Metadata struct {
	sm sync.Map
}

// LeaderUpdated updates the inmemory store with new raftio.LeaderInfo and implements raftio.IRaftEventListener.
func (m *Metadata) LeaderUpdated(info raftio.LeaderInfo) {
	m.sm.Store(info.ClusterID, info)
}

// Get queries the inmemory store for raftio.LeaderInfo by the clusterID (safe for concurrent use).
func (m *Metadata) Get(clusterID uint64) raftio.LeaderInfo {
	v, _ := m.sm.LoadOrStore(clusterID, raftio.LeaderInfo{})
	return v.(raftio.LeaderInfo)
}
