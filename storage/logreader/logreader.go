package logreader

import (
	"context"
	"fmt"
	"sync"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
	serrors "github.com/wandera/regatta/storage/errors"
)

type LogReader struct {
	ShardCacheSize int
	NodeHost       *dragonboat.NodeHost
	shardCache     sync.Map
}

// QueryRaftLog for all the entries in a given cluster within the right half-open range
// defined by dragonboat.LogRange. MaxSize denotes the maximum cumulative size of the entries,
// but this serves only as a hint and the actual size of returned entries may be larger than maxSize.
func (l *LogReader) QueryRaftLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
	// Try to read the commands from the cache first.
	cache := l.getCache(clusterID)
	size, entries, prependIndices, appendIndices := cache.Get(logRange, int(maxSize))

	if prependIndices.FirstIndex != 0 && prependIndices.LastIndex != 0 {
		// We have to query the log for the beginning of the range and prepend the cached entries.
		le, err := l.readLog(ctx, clusterID, prependIndices, maxSize-size)
		if err != nil {
			return nil, err
		}
		entries = append(le, entries...)
	}

	if appendIndices.FirstIndex != 0 && appendIndices.LastIndex != 0 {
		// We have to query the log for the end of the range and append the cached entries.
		le, err := l.readLog(ctx, clusterID, appendIndices, maxSize-size)
		if err != nil {
			return nil, err
		}
		entries = append(entries, le...)
	}

	// Update the cache.
	if len(entries) != 0 {
		cache.Put(entries)
	}

	return entries, nil
}

func (l *LogReader) NodeDeleted(info raftio.NodeInfo) {
	l.shardCache.Delete(info.ShardID)
}

func (l *LogReader) NodeReady(info raftio.NodeInfo) {
	l.shardCache.Store(info.ShardID, newCache(l.ShardCacheSize))
}

func (l *LogReader) getCache(clusterID uint64) *Cache {
	if entry, ok := l.shardCache.Load(clusterID); ok {
		return entry.(*Cache)
	}

	entry, _ := l.shardCache.LoadOrStore(clusterID, newCache(l.ShardCacheSize))
	return entry.(*Cache)
}

func (l *LogReader) readLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
	rs, err := l.NodeHost.QueryRaftLog(clusterID, logRange.FirstIndex, logRange.LastIndex, maxSize)
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
