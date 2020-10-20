package storage

import (
	"fmt"
	"net"
	"time"

	cvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/vfs"
	"github.com/wandera/regatta/raft"
	"go.uber.org/zap"
)

func startRaftNode() (*dragonboat.NodeHost, *raft.Metadata) {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	meta := &raft.Metadata{}
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 1,
		RaftAddress:    testNodeAddress,
		FS:             vfs.NewMem(),
		Expert: config.ExpertConfig{
			ExecShards:  1,
			LogDBShards: 1,
		},
		RaftEventListener: meta,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		zap.S().Panic(err)
	}

	cc := config.Config{
		NodeID:             1,
		ClusterID:          1,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10000,
		CompactionOverhead: 5000,
	}

	err = nh.StartOnDiskCluster(map[uint64]string{1: testNodeAddress}, false, func(clusterId uint64, nodeId uint64) sm.IOnDiskStateMachine {
		return raft.NewPebbleStateMachine(clusterId, nodeId, "smdir", "smwal", cvfs.NewMem())
	}, cc)
	if err != nil {
		zap.S().Panic(err)
	}

	ready := make(chan struct{}, 1)
	go func() {
		for {
			i := nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
			if i.ClusterInfoList[0].IsLeader {
				ready <- struct{}{}
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// Listen on our channel AND a timeout channel - which ever happens first.
	select {
	case <-ready:
		zap.S().Info("Test Dragonboat instance ready")
	case <-time.After(30 * time.Second):
		zap.S().Panic("Unable to start test Dragonboat in timeout of 30s")
	}

	return nh, meta
}

func getTestPort() int {
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
