// Copyright JAMF Software, LLC

package logreader

import (
	"sort"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftpb"
)

type cache struct {
	// buffer contains at most `size` raftpb.Entries all sorted
	// in ascending order by the raftpb.Entry.Index.
	buffer []raftpb.Entry
	size   int
}

func newCache(size int) *cache {
	return &cache{
		buffer: make([]raftpb.Entry, 0, size),
		size:   size,
	}
}

func (c *cache) len() int {
	return len(c.buffer)
}

// Put entries to the cache. Caller must make sure that the commands are sorted by
// raftpb.Entry.Index in ascending order when calling this function.
// When the cache is full, entries with the smallest index are evicted.
func (c *cache) put(entries []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}
	if len(entries) > c.size {
		// Consider only the commands with the highest leader index,
		// which are the ones at the back of the slice.
		entries = entries[len(entries)-c.size:]
	}

	maxIndex := c.largestIndex()

	if maxIndex == 0 {
		// There are no entries in the cache, add all inserted entries.
		c.makeRoomAndAppend(entries)
		return
	}

	i := findIndex(entries, func(index uint64) bool { return index > maxIndex })
	if i == len(entries) {
		return
	}

	c.makeRoomAndAppend(entries[i:])
}

func (c *cache) makeRoomAndAppend(entries []raftpb.Entry) {
	if c.size < (len(entries) + len(c.buffer)) {
		c.buffer = c.buffer[len(entries)+len(c.buffer)-c.size:]
	}
	c.buffer = append(c.buffer, entries...)
}

// findIndex finds the first entry in entries with index matching the supplied func.
// The supplied func can use only comparison operators and must be called over sorted entries list.
func findIndex(entries []raftpb.Entry, f func(index uint64) bool) int {
	// Short circuit if first entry is not matching func, avoids binary search.
	if f(entries[0].Index) {
		return 0
	}
	// Short circuit if last entry does not match func, avoids binary search.
	if !f(entries[len(entries)-1].Index) {
		return len(entries)
	}
	return sort.Search(len(entries), func(i int) bool { return f(entries[i].Index) })
}

// Get all the entries with the index within the supplied right half-open range dragonboat.LogRange
// [firstIndex, lastIndex).
func (c *cache) get(logRange raft.LogRange) ([]raftpb.Entry, raft.LogRange, raft.LogRange) {
	var (
		prependIndices = raft.LogRange{}
		appendIndices  = raft.LogRange{}
	)

	if len(c.buffer) == 0 {
		// There is nothing in the cache.
		return nil, logRange, appendIndices
	}

	smallestIndex := c.smallestIndex()
	if smallestIndex > logRange.LastIndex {
		// No queried entries are in the cache.
		return nil, logRange, appendIndices
	}

	largestIndex := c.largestIndex()
	if largestIndex < logRange.FirstIndex {
		// No queried entries are in the cache.
		return nil, prependIndices, logRange
	}

	start := findIndex(c.buffer, func(index uint64) bool { return index >= logRange.FirstIndex })
	end := findIndex(c.buffer, func(index uint64) bool { return index >= logRange.LastIndex })

	entries := c.buffer[start:end]

	if len(entries) != 0 {
		if logRange.FirstIndex < smallestIndex {
			prependIndices.FirstIndex = logRange.FirstIndex
			prependIndices.LastIndex = smallestIndex
		}

		if logRange.LastIndex > largestIndex+1 {
			appendIndices.FirstIndex = largestIndex + 1
			appendIndices.LastIndex = logRange.LastIndex
		}
	}

	return entries, prependIndices, appendIndices
}

// smallestIndex returns the smallest index in the cache if the cache is not empty.
func (c *cache) smallestIndex() uint64 {
	if len(c.buffer) == 0 {
		return 0
	}
	return c.buffer[0].Index
}

// largestIndex returns the largest index in the cache if the cache is not empty.
func (c *cache) largestIndex() uint64 {
	if len(c.buffer) == 0 {
		return 0
	}

	return c.buffer[len(c.buffer)-1].Index
}
