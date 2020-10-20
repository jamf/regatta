package raft

import (
	"testing"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/stretchr/testify/require"
)

func TestMetadata_Parallel(t *testing.T) {
	m := &Metadata{}
	for i := 1; i < 1000; i++ {
		t.Run("Metadata concurrent access", func(t *testing.T) {
			r := require.New(t)
			t.Parallel()

			info := raftio.LeaderInfo{
				ClusterID: uint64(i * 2),
				NodeID:    uint64(i),
				Term:      1,
				LeaderID:  1,
			}
			m.LeaderUpdated(info)

			r.Equal(info, m.Get(info.ClusterID))
			r.Equal(raftio.LeaderInfo{}, m.Get(0))
		})
	}
}
