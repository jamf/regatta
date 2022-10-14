package logreader

import (
	"sort"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
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
// [firstIndex, lastIndex).
func (c *cache) get(logRange dragonboat.LogRange) ([]raftpb.Entry, dragonboat.LogRange, dragonboat.LogRange) {
	var (
		prependIndices = dragonboat.LogRange{}
		appendIndices  = dragonboat.LogRange{}
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

	start := sort.Search(len(c.buffer), func(i int) bool { return c.buffer[i].Index >= logRange.FirstIndex })
	end := sort.Search(len(c.buffer), func(i int) bool { return c.buffer[i].Index >= logRange.LastIndex })

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

// Resize the cache to the maximum possible size.
func (c *cache) resize() {
	if len(c.buffer) > c.size {
		c.buffer = c.buffer[len(c.buffer)-c.size:]
	}
}
