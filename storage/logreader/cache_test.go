// Copyright JAMF Software, LLC

package logreader

import (
	"testing"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	c := newCache(100)
	r := require.New(t)
	{
		t.Log("insert 50 entries")
		c.put(createEntries(1, 50))
		e, _, _ := c.get(raft.LogRange{FirstIndex: 1, LastIndex: 51})
		r.Len(e, 50)
		r.Equal(uint64(1), e[0].Index)
		r.Equal(uint64(50), e[len(e)-1].Index)
	}

	{
		t.Log("inserting 70 entries evicts old entries")
		c.put(createEntries(101, 170))
		e, _, _ := c.get(raft.LogRange{FirstIndex: 1, LastIndex: 171})
		r.Len(e, 100)
		r.Equal(uint64(21), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index)
	}

	{
		t.Log("get out of range left")
		e, prep, _ := c.get(raft.LogRange{FirstIndex: 1, LastIndex: 20})
		r.Empty(e)
		r.Equal(uint64(1), prep.FirstIndex)
		r.Equal(uint64(20), prep.LastIndex)
	}

	{
		t.Log("get out of range right")
		e, _, app := c.get(raft.LogRange{FirstIndex: 200, LastIndex: 250})
		r.Empty(e)
		r.Equal(uint64(200), app.FirstIndex)
		r.Equal(uint64(250), app.LastIndex)
	}

	// From this point forward, there are only entries with indices [21, 22, ..., 50, 101, 102, ..., 170]
	// present in the cache.
	{
		t.Log("inserting 20 stale entries does not evict fresher entries")
		c.put(createEntries(1, 20))
		e, _, _ := c.get(raft.LogRange{FirstIndex: 1, LastIndex: 171})
		r.Len(e, 100)
		r.Equal(uint64(21), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index)
	}

	{
		t.Log("query subset of the cache - cache does not suggest to query log")
		fromIndex := uint64(21) // There is an entry with index 21 in the cache - cache must not suggest to look into the log.
		toIndex := uint64(151)  // There are entries with index larger than 151 - cache must not suggest to look into the log.
		e, prependIndices, appendIndices := c.get(raft.LogRange{FirstIndex: fromIndex, LastIndex: toIndex})
		r.Len(e, 80)

		// LogRange defines the right half-open interval.
		r.Equal(fromIndex, e[0].Index)
		r.Equal(toIndex-1, e[len(e)-1].Index)

		// All queried entries are present in the cache, there is no need to query the log.
		r.Equal(raft.LogRange{}, prependIndices)
		r.Equal(raft.LogRange{}, appendIndices)
	}

	{
		t.Log("query beginning of the cache - cache suggests to query log")
		fromIndex := uint64(10) // There are no entries with index smaller than 10 - cache MUST suggest to look into the log.
		toIndex := uint64(51)   // There are entries with index larger than 50 - cache must not suggest to look into the log.
		e, prependIndices, appendIndices := c.get(raft.LogRange{FirstIndex: fromIndex, LastIndex: toIndex})
		r.Len(e, 30)

		// LogRange defines the right half-open interval.
		r.Equal(uint64(21), e[0].Index) // 21 is the smallest index in the cache.
		r.Equal(toIndex-1, e[len(e)-1].Index)

		r.Equal(raft.LogRange{FirstIndex: 10, LastIndex: 21}, prependIndices) // cache suggests to look into the log up until the index 21.
		r.Equal(raft.LogRange{}, appendIndices)
	}

	{
		t.Log("query end of the cache - cache suggests to query log")
		fromIndex := uint64(151) // There are entries with index smaller than 151 - cache must not suggest to look into the log.
		toIndex := uint64(181)   // There are no entries with index larger than 180 - cache MUST suggest to look into the log.
		e, prependIndices, appendIndices := c.get(raft.LogRange{FirstIndex: fromIndex, LastIndex: toIndex})
		r.Len(e, 20)

		// LogRange defines the right half-open interval.
		r.Equal(uint64(151), e[0].Index)
		r.Equal(uint64(170), e[len(e)-1].Index) // 170 is the largest index in the cache.

		r.Equal(raft.LogRange{}, prependIndices)
		r.Equal(raft.LogRange{FirstIndex: 171, LastIndex: 181}, appendIndices) // cache suggests to look into the log starting from the index 171.
	}

	{
		t.Log("query the entire cache - cache suggests to query log")
		fromIndex := uint64(10) // There are no entries with index smaller than 10 - cache MUST suggest to look into the log.
		toIndex := uint64(181)  // There are no entries with index larger than 180 - cache MUST suggest to look into the log.
		e, prependIndices, appendIndices := c.get(raft.LogRange{FirstIndex: fromIndex, LastIndex: toIndex})
		r.Len(e, 100)

		// LogRange defines the right half-open interval.
		r.Equal(uint64(21), e[0].Index)         // Is the smallest index in the cache.
		r.Equal(uint64(170), e[len(e)-1].Index) // 170 is the largest index in the cache.

		r.Equal(raft.LogRange{FirstIndex: 10, LastIndex: 21}, prependIndices)  // cache suggests to look into the log up until the index 21.
		r.Equal(raft.LogRange{FirstIndex: 171, LastIndex: 181}, appendIndices) // cache suggests to look into the log starting from the index 171.
	}
}

func createEntries(begin, end uint64) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, end-begin)
	for i := begin; i <= end; i++ {
		entries = append(entries, raftpb.Entry{
			Index: i,
		})
	}
	return entries
}
