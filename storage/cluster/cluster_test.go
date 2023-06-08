// Copyright JAMF Software, LLC

package cluster

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jamf/regatta/util"
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
)

func TestSingleNodeCluster(t *testing.T) {
	address := getTestBindAddress()
	cluster, err := New(address, "", func() Info { return Info{} })
	require.NoError(t, err)
	cluster.Start([]string{address})
	require.Equal(t, 1, len(cluster.Nodes()))
}

func TestMultiNodeCluster(t *testing.T) {
	clusters := make(map[string]*Cluster)
	t.Log("start 3 node cluster")
	for i := 0; i < 3; i++ {
		address := getTestBindAddress()
		cluster, err := New(address, address, func() Info {
			return Info{
				NodeHostID:  util.RandString(64),
				NodeID:      uint64(i),
				RaftAddress: fmt.Sprintf("127.0.0.%d:5762", i),
				ShardInfoList: []dragonboat.ShardInfo{
					{
						Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.2:5762", 3: "127.0.0.3:5762"},
						ShardID:           1,
						ReplicaID:         1,
						ConfigChangeIndex: 1,
						LeaderID:          1,
						Term:              5,
					},
					{
						Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.2:5762", 3: "127.0.0.3:5762"},
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
		cluster.Start(maps.Keys(clusters))
		require.Equal(t, i+1, len(cluster.Nodes()))
	}

	t.Log("all members see the others and has the same view of the world")
	for _, cluster1 := range clusters {
		nodes := cluster1.Nodes()
		for _, cluster2 := range clusters {
			require.ElementsMatch(t, nodes, cluster2.Nodes())
		}
		require.Equal(t, dragonboat.ShardView{
			ShardID:           1,
			Nodes:             map[uint64]string{1: "127.0.0.1:5762", 2: "127.0.0.2:5762", 3: "127.0.0.3:5762"},
			ConfigChangeIndex: 1,
			LeaderID:          1,
			Term:              5,
		}, cluster1.ShardInfo(1))
	}
	c1 := maps.Values(clusters)[0]
	c2 := maps.Values(clusters)[1]

	t.Log("test prefix watch")
	recvChan := make(chan Message)
	c2.WatchPrefix("test-", func(message Message) {
		recvChan <- message
	})
	require.NoError(t, c1.SendTo(c2.LocalNode(), Message{Key: "test-foo", Payload: nil}))
	require.Eventually(t, func() bool {
		m := <-recvChan
		return strings.HasPrefix(m.Key, "test-")
	}, 5*time.Second, 100*time.Millisecond)

	t.Log("test key watch")
	recvChan = make(chan Message)
	c2.WatchKey("specific-key", func(message Message) {
		recvChan <- message
	})
	require.NoError(t, c1.SendTo(c2.LocalNode(), Message{Key: "specific-key", Payload: nil}))
	require.Eventually(t, func() bool {
		m := <-recvChan
		return m.Key == "specific-key"
	}, 5*time.Second, 100*time.Millisecond)

	t.Log("test broadcast")
	count := atomic.NewUint32(0)
	for _, cluster := range clusters {
		cluster.WatchKey("broadcast", func(message Message) {
			count.Add(1)
		})
	}
	// Wait for cluster stabilisation before broadcasting.
	time.Sleep(5 * time.Second)
	c1.Broadcast(Message{Key: "broadcast"})
	require.Eventually(t, func() bool {
		return int(count.Load()) >= len(clusters)
	}, 10*time.Second, 100*time.Millisecond)

	t.Log("shutdown all members")
	for _, cluster := range clusters {
		require.NoError(t, cluster.Close())
	}
}

func getTestBindAddress() string {
	l, _ := net.Listen("tcp4", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().String()
}
