// Copyright JAMF Software, LLC

package storage

import (
	"github.com/lni/dragonboat/v4/raftio"
)

func (e *Engine) dispatchEvents() {
	for {
		select {
		case <-e.stop:
			return
		case evt := <-e.eventsCh:
			e.log.Infof("raft: %T %+v", evt, evt)
			switch ev := evt.(type) {
			case leaderUpdated, nodeUnloaded, membershipChanged, nodeHostShuttingDown:
				e.Cluster.Notify()
			case nodeReady:
				if ev.ReplicaID == e.cfg.NodeID && e.LogCache != nil {
					e.LogCache.NodeReady(ev.ShardID)
				}
				e.Cluster.Notify()
			case nodeDeleted:
				if ev.ReplicaID == e.cfg.NodeID && e.LogCache != nil {
					e.LogCache.NodeDeleted(ev.ShardID)
				}
				e.Cluster.Notify()
			case logCompacted:
				if ev.ReplicaID == e.cfg.NodeID && e.LogCache != nil {
					e.LogCache.LogCompacted(ev.ShardID)
				}
				e.Cluster.Notify()
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

func (e *Engine) LeaderUpdated(info raftio.LeaderInfo) {
	e.eventsCh <- leaderUpdated{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		Term:      info.Term,
		LeaderID:  info.LeaderID,
	}
}

type nodeHostShuttingDown struct{}

func (e *Engine) NodeHostShuttingDown() {
	e.eventsCh <- nodeHostShuttingDown{}
}

type nodeUnloaded struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) NodeUnloaded(info raftio.NodeInfo) {
	e.eventsCh <- nodeUnloaded{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}

type nodeDeleted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) NodeDeleted(info raftio.NodeInfo) {
	e.eventsCh <- nodeDeleted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}

type nodeReady struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) NodeReady(info raftio.NodeInfo) {
	e.eventsCh <- nodeReady{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}

type membershipChanged struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) MembershipChanged(info raftio.NodeInfo) {
	e.eventsCh <- membershipChanged{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}

type connectionEstablished struct {
	Address            string
	SnapshotConnection bool
}

func (e *Engine) ConnectionEstablished(info raftio.ConnectionInfo) {
	e.eventsCh <- connectionEstablished{
		Address:            info.Address,
		SnapshotConnection: info.SnapshotConnection,
	}
}

type connectionFailed struct {
	Address            string
	SnapshotConnection bool
}

func (e *Engine) ConnectionFailed(info raftio.ConnectionInfo) {
	e.eventsCh <- connectionFailed{
		Address:            info.Address,
		SnapshotConnection: info.SnapshotConnection,
	}
}

type sendSnapshotStarted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SendSnapshotStarted(info raftio.SnapshotInfo) {
	e.eventsCh <- sendSnapshotStarted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type sendSnapshotCompleted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	e.eventsCh <- sendSnapshotCompleted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type sendSnapshotAborted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SendSnapshotAborted(info raftio.SnapshotInfo) {
	e.eventsCh <- sendSnapshotAborted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type snapshotReceived struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SnapshotReceived(info raftio.SnapshotInfo) {
	e.eventsCh <- snapshotReceived{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type snapshotRecovered struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SnapshotRecovered(info raftio.SnapshotInfo) {
	e.eventsCh <- snapshotRecovered{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type snapshotCreated struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SnapshotCreated(info raftio.SnapshotInfo) {
	e.eventsCh <- snapshotCreated{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type snapshotCompacted struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

func (e *Engine) SnapshotCompacted(info raftio.SnapshotInfo) {
	e.eventsCh <- snapshotCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		From:      info.From,
		Index:     info.Index,
	}
}

type logCompacted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) LogCompacted(info raftio.EntryInfo) {
	e.eventsCh <- logCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}

type logDBCompacted struct {
	ShardID   uint64
	ReplicaID uint64
}

func (e *Engine) LogDBCompacted(info raftio.EntryInfo) {
	e.eventsCh <- logDBCompacted{
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
	}
}
