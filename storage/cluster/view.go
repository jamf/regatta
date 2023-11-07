// Copyright JAMF Software, LLC

package cluster

import (
	"math/rand"
	"sync"

	"github.com/lni/dragonboat/v4"
)

// noLeader if shard has no leader the value will be equal to this constant (as IDs must be >=1).
const noLeader = 0

type shardView struct {
	mtx    sync.RWMutex
	shards map[uint64]dragonboat.ShardView
}

func newView() *shardView {
	return &shardView{
		shards: make(map[uint64]dragonboat.ShardView),
	}
}

func mergeShardInfo(current dragonboat.ShardView, update dragonboat.ShardView) dragonboat.ShardView {
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

func toShardViewList(input []dragonboat.ShardInfo) []dragonboat.ShardView {
	result := make([]dragonboat.ShardView, len(input))
	for i, ci := range input {
		result[i] = dragonboat.ShardView{
			ShardID:           ci.ShardID,
			Replicas:          ci.Replicas,
			ConfigChangeIndex: ci.ConfigChangeIndex,
			LeaderID:          ci.LeaderID,
			Term:              ci.Term,
		}
	}
	return result
}

func (v *shardView) update(updates []dragonboat.ShardView) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	for _, u := range updates {
		current, ok := v.shards[u.ShardID]
		if !ok {
			current = dragonboat.ShardView{ShardID: u.ShardID}
		}
		v.shards[u.ShardID] = mergeShardInfo(current, u)
	}
}

func (v *shardView) copy() []dragonboat.ShardView {
	v.mtx.RLock()
	defer v.mtx.RUnlock()
	ci := make([]dragonboat.ShardView, 0, len(v.shards))
	for _, v := range v.shards {
		ci = append(ci, v)
	}
	rand.Shuffle(len(ci), func(i, j int) { ci[i], ci[j] = ci[j], ci[i] })
	return ci
}

func (v *shardView) shardInfo(id uint64) dragonboat.ShardView {
	v.mtx.RLock()
	defer v.mtx.RUnlock()
	return v.shards[id]
}
