// Copyright JAMF Software, LLC

package storage

import (
	"github.com/jamf/regatta/raft/raftio"
)

type events struct {
	eventsCh chan any
	stopc    chan struct{}
	engine   *Engine
}

func (e *events) dispatchEvents() {
	for evt := range e.eventsCh {
		e.engine.log.Infof("raft: %T %+v", evt, evt)
		switch ev := evt.(type) {
		case nodeHostShuttingDown:
			close(e.stopc)
			return
		case leaderUpdated, nodeUnloaded, membershipChanged, nodeReady:
			e.engine.Cluster.Notify()
		case nodeDeleted:
			if ev.ReplicaID == e.engine.cfg.NodeID && e.engine.LogCache != nil {
				e.engine.LogCache.NodeDeleted(ev.ShardID)
			}
			e.engine.Cluster.Notify()
		case logCompacted:
			if ev.ReplicaID == e.engine.cfg.NodeID && e.engine.LogCache != nil {
				e.engine.LogCache.LogCompacted(ev.ShardID)
			}
		}
	}
}

type leaderUpdated struct {
	ShardID   uint64
	ReplicaID uint64
	Term      uint64
	LeaderID  uint64
}

func (e *events) LeaderUpdated(info raftio.LeaderInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- leaderUpdated{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		Term:      info.Term,
		LeaderID:  info.LeaderID,
	}:
	}
}

type nodeHostShuttingDown struct{}

func (e *events) NodeHostShuttingDown() {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- nodeHostShuttingDown{}:
	}
}

type nodeUnloaded struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) NodeUnloaded(info raftio.NodeInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- nodeUnloaded{ShardID: info.ShardID, ReplicaID: info.ReplicaID}:
	}
}

type nodeDeleted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) NodeDeleted(info raftio.NodeInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- nodeDeleted{ShardID: info.ShardID, ReplicaID: info.ReplicaID}:
	}
}

type nodeReady struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) NodeReady(info raftio.NodeInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- nodeReady{ShardID: info.ShardID, ReplicaID: info.ReplicaID}:
	}
}

type membershipChanged struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) MembershipChanged(info raftio.NodeInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- membershipChanged{ShardID: info.ShardID, ReplicaID: info.ReplicaID}:
	}
}

type connectionEstablished struct {
	Address            string
	SnapshotConnection bool
}

func (e *events) ConnectionEstablished(info raftio.ConnectionInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- connectionEstablished{Address: info.Address, SnapshotConnection: info.SnapshotConnection}:
	}
}

type connectionFailed struct {
	Address            string
	SnapshotConnection bool
}

func (e *events) ConnectionFailed(info raftio.ConnectionInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- connectionFailed{Address: info.Address, SnapshotConnection: info.SnapshotConnection}:
	}
}

type sendSnapshotStarted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SendSnapshotStarted(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- sendSnapshotStarted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type sendSnapshotCompleted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- sendSnapshotCompleted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type sendSnapshotAborted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SendSnapshotAborted(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- sendSnapshotAborted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type snapshotReceived struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SnapshotReceived(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- snapshotReceived{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type snapshotRecovered struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SnapshotRecovered(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- snapshotRecovered{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type snapshotCreated struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SnapshotCreated(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- snapshotCreated{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type snapshotCompacted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *events) SnapshotCompacted(info raftio.SnapshotInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- snapshotCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}:
	}
}

type logCompacted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) LogCompacted(info raftio.EntryInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- logCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}:
	}
}

type logDBCompacted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *events) LogDBCompacted(info raftio.EntryInfo) {
	select {
	case <-e.stopc:
		return
	case e.eventsCh <- logDBCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}:
	}
}
