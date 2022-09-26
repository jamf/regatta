package logreader

import (
	"sort"
	"sync"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
)

type Cache struct {
	// buffer contains at most `size` raftpb.Entries all sorted
	// in ascending order by the raftpb.Entry.Index.
	buffer []raftpb.Entry
	size   int
	mtx    sync.RWMutex
}

func newCache(size int) *Cache {
	return &Cache{
		buffer: make([]raftpb.Entry, 0, size),
		size:   size,
	}
}

// Put entries to the cache. Caller must make sure that the commands are sorted by
// raftpb.Entry.Index in ascending order when calling this function.
// When the cache is full, entries with the smallest index are evicted.
func (c *Cache) Put(entries []raftpb.Entry) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if len(entries) > c.size {
		// Consider only the commands with the highest leader index,
		// which are the ones at the back of the slice.
		entries = entries[len(entries)-int(c.size):]
	}

	maxIndex := c.largestIndex()
	if maxIndex == 0 {
		// There are no entries in the cache, add all inserted entries.
		c.buffer = append(c.buffer, entries...)
		c.resize()
		return
	}

	// Find the first entry with index greater than the largest index in the cache.
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Index > maxIndex })
	if i == len(entries) {
		return
	}

	c.buffer = append(c.buffer, entries[i:]...)
	c.resize()
}

// Get all the entries with the index within the supplied right half-open range dragonboat.LogRange
// [firstIndex, lastIndex). Approximate size of all the entries and entries sorted in ascending order are returned.
func (c *Cache) Get(logRange dragonboat.LogRange, maxSize int) (uint64, []raftpb.Entry, dragonboat.LogRange, dragonboat.LogRange) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	var (
		prependIndices = dragonboat.LogRange{}
		appendIndices  = dragonboat.LogRange{}
		totalSize      = 0
	)

	if len(c.buffer) == 0 {
		// There is nothing in the cache.
		return 0, nil, logRange, appendIndices
	}

	smallestIndex := c.smallestIndex()
	if smallestIndex > logRange.LastIndex {
		// No queried entries are in the cache.
		return 0, nil, logRange, appendIndices
	}

	largestIndex := c.largestIndex()
	if largestIndex < logRange.FirstIndex {
		// No queried entries are in the cache.
		return 0, nil, prependIndices, logRange
	}

	entries := make([]raftpb.Entry, 0, logRange.LastIndex-logRange.FirstIndex)
	i := sort.Search(len(c.buffer), func(i int) bool { return c.buffer[i].Index >= logRange.FirstIndex })
	for _, entry := range c.buffer[i:] {
		if entry.Index < logRange.FirstIndex {
			continue
		}

		if entry.Index >= logRange.LastIndex || totalSize > maxSize {
			break
		}

		totalSize += entry.SizeUpperLimit()
		entries = append(entries, entry)
	}

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

	return uint64(totalSize), entries, prependIndices, appendIndices
}

// smallestIndex returns the smallest index in the cache if the cache is not empty.
func (c *Cache) smallestIndex() uint64 {
	if len(c.buffer) == 0 {
		return 0
	}
	return c.buffer[0].Index
}

// largestIndex returns the largest index in the cache if the cache is not empty.
func (c *Cache) largestIndex() uint64 {
	if len(c.buffer) == 0 {
		return 0
	}

	return c.buffer[len(c.buffer)-1].Index
}

// Resize the cache to the maximum possible size.
func (c *Cache) resize() {
	if len(c.buffer) > c.size {
		c.buffer = c.buffer[len(c.buffer)-c.size:]
	}
}
