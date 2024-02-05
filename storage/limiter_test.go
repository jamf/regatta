// Copyright JAMF Software, LLC

package storage

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jamf/regatta/storage/kv"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestKvLimiter_Basic(t *testing.T) {
	mc := clock.NewMock()
	l := &kvLimiter{rs: newRaftStore(t), clock: mc}
	tok, rem, err := l.Get("foo")
	require.NoError(t, err)
	require.Equal(t, uint64(0), tok)
	require.Equal(t, uint64(0), rem)

	err = l.Set("foo", 10, time.Second)
	require.NoError(t, err)

	tok, rem, err = l.Get("foo")
	require.NoError(t, err)
	require.Equal(t, uint64(10), tok)

	rem, _, ok, err := l.Take("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(9), rem)

	mc.Add(5 * time.Second)

	rem, _, ok, err = l.Take("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(58), rem)
}

func TestKvLimiter_Burst(t *testing.T) {
	mc := clock.NewMock()
	l := &kvLimiter{rs: newRaftStore(t), clock: mc}
	tok, rem, err := l.Get("foo")
	require.NoError(t, err)
	require.Equal(t, uint64(0), tok)
	require.Equal(t, uint64(0), rem)

	err = l.Set("foo", 1, time.Second)
	require.NoError(t, err)

	tok, rem, err = l.Get("foo")
	require.NoError(t, err)
	require.Equal(t, uint64(1), tok)

	rem, _, ok, err := l.Take("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), rem)

	require.NoError(t, l.Burst("foo", 5, time.Second, 30*time.Second))

	mc.Add(5 * time.Second)

	rem, _, ok, err = l.Take("foo")
	require.NoError(t, err)
	require.True(t, ok)
	// (0 + 5 * 5/s) - 1.
	require.Equal(t, uint64(24), rem)

	// Missed additional burst tick
	// (24 + 30 * 1/s) - 1
	mc.Add(30 * time.Second)

	rem, _, ok, err = l.Take("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(53), rem)
}

func TestKvLimiter_WaitFor(t *testing.T) {
	l := &kvLimiter{rs: newRaftStore(t), clock: clock.New()}
	tok, rem, err := l.Get("foo")
	require.NoError(t, err)
	require.Equal(t, uint64(0), tok)
	require.Equal(t, uint64(0), rem)

	err = l.Set("foo", 1, time.Second)
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	require.NoError(t, l.WaitFor(ctx, "foo"))
	require.NoError(t, l.WaitFor(ctx, "foo"))
	require.NoError(t, l.WaitFor(ctx, "foo"))
	require.NoError(t, l.WaitFor(ctx, "foo"))
	require.NoError(t, l.WaitFor(ctx, "foo"))
	require.ErrorIs(t, l.WaitFor(ctx, "foo"), context.DeadlineExceeded)
}

func newRaftStore(t *testing.T) *kv.RaftStore {
	t.Helper()
	getTestPort := func() int {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}

	startRaftNode := func() *dragonboat.NodeHost {
		testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
		nhc := config.NodeHostConfig{
			WALDir:         "wal",
			NodeHostDir:    "dragonboat",
			RTTMillisecond: 1,
			RaftAddress:    testNodeAddress,
		}
		_ = nhc.Prepare()
		nhc.Expert.FS = vfs.NewMem()
		nhc.Expert.Engine.ExecShards = 1
		nhc.Expert.LogDB.Shards = 1
		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}

		cc := config.Config{
			ReplicaID:          1,
			ShardID:            1,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
			WaitReady:          true,
		}

		err = nh.StartConcurrentReplica(map[uint64]string{1: testNodeAddress}, false, kv.NewLFSM(), cc)
		if err != nil {
			panic(err)
		}
		require.Eventually(t, func() bool {
			_, _, ok, _ := nh.GetLeaderID(cc.ReplicaID)
			return ok
		}, 5*time.Second, 10*time.Millisecond)
		return nh
	}

	node := startRaftNode()
	t.Cleanup(node.Close)
	return &kv.RaftStore{NodeHost: node, ClusterID: 1}
}
