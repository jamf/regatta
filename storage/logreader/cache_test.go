package logreader

import (
	"testing"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/regattaserver"
)

func TestCache(t *testing.T) {
	r := require.New(t)
	c := newCache(100)

	t.Run("insert 50 entries", func(t *testing.T) {
		c.Put(createEntries(1, 50))
		size, e, _, _ := c.Get(dragonboat.LogRange{FirstIndex: 1, LastIndex: 51}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(50, len(e))
		r.Equal(uint64(1), e[0].Index)
		r.Equal(uint64(50), e[len(e)-1].Index)
	})

	t.Run("inserting 70 entries evicts old entries", func(t *testing.T) {
		c.Put(createEntries(101, 170))
		size, e, _, _ := c.Get(dragonboat.LogRange{FirstIndex: 1, LastIndex: 171}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(100, len(e))
		r.Equal(uint64(21), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index)
	})
	// From this point forward, there are only entries with indices [21, 22, ..., 50, 101, 102, ..., 170]
	// present in the cache.

	t.Run("inserting 20 stale entries does not evict fresher entries", func(t *testing.T) {
		c.Put(createEntries(1, 20))
		size, e, _, _ := c.Get(dragonboat.LogRange{FirstIndex: 1, LastIndex: 171}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(100, len(e))
		r.Equal(uint64(21), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index)
	})

	t.Run("query subset of the cache - cache does not suggest to query log", func(t *testing.T) {
		fromIndex := uint64(21) // There is an entry with index 21 in the cache - cache must not suggest to look into the log.
		toIndex := uint64(151)  // There are entries with index larger than 151 - cache must not suggest to look into the log.
		size, e, prependIndices, appendIndices := c.Get(dragonboat.LogRange{FirstIndex: fromIndex, LastIndex: toIndex}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(80, len(e))

		// LogRange defines the right half-open interval.
		r.Equal(fromIndex, e[0].Index)
		r.Equal(toIndex-1, e[len(e)-1].Index)

		// All queried entries are present in the cache, there is no need to query the log.
		r.Equal(dragonboat.LogRange{}, prependIndices)
		r.Equal(dragonboat.LogRange{}, appendIndices)
	})

	t.Run("query beginning of the cache - cache suggests to query log", func(t *testing.T) {
		fromIndex := uint64(10) // There are no entries with index smaller than 10 - cache MUST suggest to look into the log.
		toIndex := uint64(51)   // There are entries with index larger than 50 - cache must not suggest to look into the log.
		size, e, prependIndices, appendIndices := c.Get(dragonboat.LogRange{FirstIndex: fromIndex, LastIndex: toIndex}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(30, len(e))

		// LogRange defines the right half-open interval.
		r.Equal(uint64(21), e[0].Index) // 21 is the smallest index in the cache.
		r.Equal(toIndex-1, e[len(e)-1].Index)

		r.Equal(dragonboat.LogRange{FirstIndex: 10, LastIndex: 21}, prependIndices) // Cache suggests to look into the log up until the index 21.
		r.Equal(dragonboat.LogRange{}, appendIndices)
	})

	t.Run("query end of the cache - cache suggests to query log", func(t *testing.T) {
		fromIndex := uint64(151) // There are entries with index smaller than 151 - cache must not suggest to look into the log.
		toIndex := uint64(181)   // There are no entries with index larger than 180 - cache MUST suggest to look into the log.
		size, e, prependIndices, appendIndices := c.Get(dragonboat.LogRange{FirstIndex: fromIndex, LastIndex: toIndex}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(20, len(e))

		// LogRange defines the right half-open interval.
		r.Equal(uint64(151), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index) // 170 is the largets index in the cache.

		r.Equal(dragonboat.LogRange{}, prependIndices)
		r.Equal(dragonboat.LogRange{FirstIndex: 171, LastIndex: 181}, appendIndices) // Cache suggests to look into the log starting from the index 171.
	})

	t.Run("query the entire cache - cache suggests to query log", func(t *testing.T) {
		fromIndex := uint64(10) // There are no entries with index smaller than 10 - cache MUST suggest to look into the log.
		toIndex := uint64(181)  // There are no entries with index larger than 180 - cache MUST suggest to look into the log.
		size, e, prependIndices, appendIndices := c.Get(dragonboat.LogRange{FirstIndex: fromIndex, LastIndex: toIndex}, regattaserver.DefaultMaxGRPCSize)
		r.NotEmpty(size)
		r.Equal(100, len(e))

		// LogRange defines the right half-open interval.
		r.Equal(uint64(21), e[0].Index)         // Is the smallest index in the cache.
		r.Equal(uint64(170), e[len(e)-1].Index) // 170 is the largets index in the cache.

		r.Equal(dragonboat.LogRange{FirstIndex: 10, LastIndex: 21}, prependIndices)  // Cache suggests to look into the log up until the index 21.
		r.Equal(dragonboat.LogRange{FirstIndex: 171, LastIndex: 181}, appendIndices) // Cache suggests to look into the log starting from the index 171.
	})
}

func createEntries(begin, end uint64) []raftpb.Entry {
	entries := make([]raftpb.Entry, end-begin)
	for i := begin; i <= end; i++ {
		entries = append(entries, raftpb.Entry{
			Index: i,
		})
	}
	return entries
}
