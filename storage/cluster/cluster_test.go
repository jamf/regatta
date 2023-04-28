// Copyright JAMF Software, LLC

package cluster

import (
	"fmt"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func TestSingleNodeCluster(t *testing.T) {
	address := getTestBindAddress()
	cluster, err := New(address, "", func() Info { return Info{} })
	require.NoError(t, err)
	count, err := cluster.Start([]string{address})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestMultiNodeCluster(t *testing.T) {
	clusters := make(map[string]*Cluster)
	t.Log("start 3 node cluster")
	for i := 0; i < 3; i++ {
		address := getTestBindAddress()
		cluster, err := New(address, "", func() Info {
			return Info{
				NodeHostID:  uuid.New().String(),
				NodeID:      uint64(i),
				RaftAddress: fmt.Sprintf("127.0.0.%d:5762", i),
				ShardInfoList: []dragonboat.ShardInfo{
					{
						Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.1:5762", 3: "127.0.0.1:5762"},
						ShardID:           1,
						ReplicaID:         1,
						ConfigChangeIndex: 1,
						LeaderID:          1,
						Term:              5,
					},
					{
						Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.1:5762", 3: "127.0.0.1:5762"},
						ShardID:           2,
						ReplicaID:         1,
						ConfigChangeIndex: 1,
						LeaderID:          1,
						Term:              5,
					},
				},
			}
		})
		require.NoError(t, err)
		clusters[address] = cluster
		count, err := cluster.Start(maps.Keys(clusters))
		require.NoError(t, err)
		require.Equal(t, i+1, count)
	}

	t.Log("all members see the others and has the same view of the world")
	for _, cluster1 := range clusters {
		nodes := cluster1.Nodes()
		for _, cluster2 := range clusters {
			require.ElementsMatch(t, nodes, cluster2.Nodes())
		}
		require.Equal(t, dragonboat.ShardView{
			ShardID:           1,
			Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.1:5762", 3: "127.0.0.1:5762"},
			ConfigChangeIndex: 1,
			LeaderID:          1,
			Term:              5,
		}, cluster1.ShardInfo(1))
	}

	t.Log("shutdown all members")
	for _, cluster := range clusters {
		require.NoError(t, cluster.Close())
	}
}

func getTestBindAddress() string {
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().String()
}
