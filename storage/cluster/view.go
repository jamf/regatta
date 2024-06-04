// Copyright JAMF Software, LLC

package cluster

import (
	"math/rand/v2"
	"sync"

	"github.com/jamf/regatta/raft"
)

// noLeader if shard has no leader the value will be equal to this constant (as IDs must be >=1).
const noLeader = 0

type shardView struct {
	mtx    sync.RWMutex
	shards map[uint64]raft.ShardView
}

func newView() *shardView {
	return &shardView{
		shards: make(map[uint64]raft.ShardView),
	}
}

func mergeShardInfo(current raft.ShardView, update raft.ShardView) raft.ShardView {
	if current.ConfigChangeIndex < update.ConfigChangeIndex {
		current.Replicas = update.Replicas
		current.ConfigChangeIndex = update.ConfigChangeIndex
	}
	// we only keep which replica is the last known leader
	if update.LeaderID != noLeader {
		if current.LeaderID == noLeader || update.Term > current.Term {
			current.LeaderID = update.LeaderID
			current.Term = update.Term
		}
	}

	return current
}

func toShardViewList(input []raft.ShardInfo) []raft.ShardView {
	result := make([]raft.ShardView, len(input))
	for i, ci := range input {
		result[i] = raft.ShardView{
			ShardID:           ci.ShardID,
			Replicas:          ci.Replicas,
			ConfigChangeIndex: ci.ConfigChangeIndex,
			LeaderID:          ci.LeaderID,
			Term:              ci.Term,
		}
	}
	return result
}

func (v *shardView) update(updates []raft.ShardView) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	for _, u := range updates {
		current, ok := v.shards[u.ShardID]
		if !ok {
			current = raft.ShardView{ShardID: u.ShardID}
		}
		v.shards[u.ShardID] = mergeShardInfo(current, u)
	}
}

func (v *shardView) copy() []raft.ShardView {
	v.mtx.RLock()
	defer v.mtx.RUnlock()
	ci := make([]raft.ShardView, 0, len(v.shards))
	for _, v := range v.shards {
		ci = append(ci, v)
	}
	rand.Shuffle(len(ci), func(i, j int) { ci[i], ci[j] = ci[j], ci[i] })
	return ci
}

func (v *shardView) shardInfo(id uint64) raft.ShardView {
	v.mtx.RLock()
	defer v.mtx.RUnlock()
	return v.shards[id]
}
