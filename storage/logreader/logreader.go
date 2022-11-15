// Copyright JAMF Software, LLC

package logreader

import (
	"context"
	"fmt"
	"sync"

	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/util"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
)

type logQuerier interface {
	QueryRaftLog(shardID uint64, firstIndex uint64, lastIndex uint64, maxSize uint64) (*dragonboat.RequestState, error)
}

type shard struct {
	*cache
	mtx sync.Mutex
}

type LogReader struct {
	ShardCacheSize int
	LogQuerier     logQuerier
	shardCache     util.SyncMap[uint64, *shard]
}

// QueryRaftLog for all the entries in a given cluster within the right half-open range
// defined by dragonboat.LogRange. MaxSize denotes the maximum cumulative size of the entries,
// but this serves only as a hint and the actual size of returned entries may be larger than maxSize.
func (l *LogReader) QueryRaftLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
	// Empty log range should return immediately.
	if logRange.FirstIndex == logRange.LastIndex {
		return nil, nil
	}

	// Try to read the commands from the cache first.
	sh, ok := l.shardCache.Load(clusterID)
	if !ok {
		return nil, dragonboat.ErrShardNotReady
	}
	// Lock this shard.
	sh.mtx.Lock()
	defer sh.mtx.Unlock()

	cachedEntries, prependIndices, appendIndices := sh.get(logRange)

	if prependIndices.FirstIndex != 0 && prependIndices.LastIndex != 0 {
		// We have to query the log for the beginning of the range and prepend the cached entries.
		le, err := l.readLog(ctx, clusterID, prependIndices, maxSize)
		if err != nil {
			return nil, err
		}

		// Only if cached and queried entries form a sequence append and cache them, otherwise return the prependIndices without caching.
		if len(le) == 0 {
			return fixSize(cachedEntries, maxSize), nil
		} else if len(cachedEntries) > 0 && le[len(le)-1].Index == cachedEntries[0].Index-1 {
			entries := append(le, cachedEntries...)
			return fixSize(entries, maxSize), nil
		} else {
			if sh.len() == 0 {
				sh.put(le)
			}
			return le, nil
		}
	}

	if appendIndices.FirstIndex != 0 && appendIndices.LastIndex != 0 {
		// We have to query the log for the end of the range and append the cached entries.
		le, err := l.readLog(ctx, clusterID, appendIndices, maxSize)
		if err != nil {
			return nil, err
		}
		if len(le) == 0 {
			return fixSize(cachedEntries, maxSize), nil
		} else if len(cachedEntries) > 0 {
			sh.put(le)
			return fixSize(append(cachedEntries, le...), maxSize), nil
		} else {
			// Is consecutive to the cache if so cache the range.
			if le[0].Index-1 == sh.largestIndex() {
				sh.put(le)
			}
			return le, nil
		}
	}

	return fixSize(cachedEntries, maxSize), nil
}

func (l *LogReader) NodeDeleted(info raftio.NodeInfo) {
	l.shardCache.Delete(info.ShardID)
}

func (l *LogReader) NodeReady(info raftio.NodeInfo) {
	l.shardCache.ComputeIfAbsent(info.ShardID, func(shardId uint64) *shard { return &shard{cache: newCache(l.ShardCacheSize)} })
}

func (l *LogReader) LogCompacted(info raftio.EntryInfo) {
	l.shardCache.Store(info.ShardID, &shard{cache: newCache(l.ShardCacheSize)})
}

func (l *LogReader) readLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
	rs, err := l.LogQuerier.QueryRaftLog(clusterID, logRange.FirstIndex, logRange.LastIndex, maxSize)
	if err != nil {
		return nil, err
	}
	defer rs.Release()
	select {
	case result := <-rs.AppliedC():
		switch {
		case result.Completed():
			entries, _ := result.RaftLogs()
			return entries, nil
		case result.RequestOutOfRange():
			_, rng := result.RaftLogs()
			// Follower is up-to-date with the leader, therefore there are no new data to be sent.
			if rng.LastIndex == logRange.FirstIndex {
				return nil, nil
			}
			// Follower is ahead of the leader, has to be manually fixed.
			if rng.LastIndex < logRange.FirstIndex {
				return nil, serrors.ErrLogBehind
			}
			// Follower's leaderIndex is in the leader's snapshot, not in the log.
			if logRange.FirstIndex < rng.FirstIndex {
				return nil, serrors.ErrLogAhead
			}
			return nil, fmt.Errorf("request out of range")
		case result.Timeout():
			return nil, fmt.Errorf("reading raft log timeouted")
		case result.Rejected():
			return nil, fmt.Errorf("reading raft log rejected")
		case result.Terminated():
			return nil, fmt.Errorf("reading raft log terminated")
		case result.Dropped():
			return nil, fmt.Errorf("raft log query dropped")
		case result.Aborted():
			return nil, fmt.Errorf("raft log query aborted")
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func fixSize(entries []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	size := 0
	for i := 0; i < len(entries); i++ {
		size += entries[i].SizeUpperLimit()
		if uint64(size) >= maxSize {
			return entries[:i]
		}
	}
	return entries
}
