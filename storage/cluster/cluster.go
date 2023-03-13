// Copyright JAMF Software, LLC

package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"go.uber.org/zap"
)

type clusterState struct {
	ShardView []dragonboat.ShardView `json:"ShardView"`
}

type Info struct {
	// NodeHostID is the unique identifier of the NodeHost instance.
	NodeHostID string
	// NodeID is the ID of this replica.
	NodeID uint64
	// RaftAddress is the public address of the NodeHost used for exchanging Raft
	// messages, snapshots and other metadata with other NodeHost instances.
	RaftAddress string
	// ShardInfo is a list of all Raft shards managed by the NodeHost
	ShardInfoList []dragonboat.ShardInfo
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost.
	LogInfo []raftio.NodeInfo
}

type getClusterInfo func() Info

type Cluster struct {
	ml         *memberlist.Memberlist
	infoF      getClusterInfo
	view       *view
	mtx        sync.RWMutex
	broadcasts *memberlist.TransmitLimitedQueue
	log        *zap.SugaredLogger
}

func (c *Cluster) NotifyJoin(node *memberlist.Node) {
	meta := nodeMeta{}
	_ = json.Unmarshal(node.Meta, &meta)
	c.log.Infof("%s joined", meta)
}

func (c *Cluster) NotifyLeave(node *memberlist.Node) {
	meta := nodeMeta{}
	_ = json.Unmarshal(node.Meta, &meta)
	c.log.Infof("%s left", meta)
}

func (c *Cluster) NotifyUpdate(node *memberlist.Node) {
	meta := nodeMeta{}
	_ = json.Unmarshal(node.Meta, &meta)
	c.log.Infof("%s updated", meta)
}

func (c *Cluster) Start(join []string) (int, error) {
	return c.ml.Join(join)
}

func (c *Cluster) Close() error {
	if err := c.ml.Leave(10 * time.Second); err != nil {
		return err
	}
	if err := c.ml.Shutdown(); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) getState() clusterState {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return clusterState{}
}

func New(bindAddr string, advAddr string, f getClusterInfo) (*Cluster, error) {
	info := f()
	log := zap.S().Named("memberlist").WithOptions(zap.AddCallerSkip(4))
	bus := &Cluster{log: log, infoF: f, view: newView()}

	mcfg := memberlist.DefaultLANConfig()
	mcfg.LogOutput = &loggerAdapter{log: log}
	mcfg.Name = strconv.FormatUint(info.NodeID, 10)
	mcfg.Delegate = bus
	mcfg.Events = bus

	host, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	mcfg.BindAddr = host
	mcfg.BindPort, _ = strconv.Atoi(port)

	if advAddr != "" {
		aHost, aPort, aerr := net.SplitHostPort(bindAddr)
		if aerr != nil {
			return nil, aerr
		}
		mcfg.AdvertiseAddr = aHost
		mcfg.AdvertisePort, _ = strconv.Atoi(aPort)
	}

	ml, err := memberlist.Create(mcfg)
	if err != nil {
		return nil, err
	}
	bus.ml = ml
	bus.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       bus.ml.NumMembers,
		RetransmitMult: mcfg.RetransmitMult,
	}
	return bus, err
}

type nodeMeta struct {
	ID          string `json:"ID"`
	NodeID      uint64 `json:"NodeID"`
	RaftAddress string `json:"RaftAddress"`
}

func (n nodeMeta) String() string {
	return fmt.Sprintf("[%s, %d, %s]", n.ID, n.NodeID, n.RaftAddress)
}

func (c *Cluster) NodeMeta(limit int) []byte {
	info := c.infoF()
	bytes, _ := json.Marshal(&nodeMeta{
		ID:          info.NodeHostID,
		NodeID:      info.NodeID,
		RaftAddress: info.RaftAddress,
	})
	if len(bytes) > limit {
		panic("gossip meta limit exceeded")
	}
	return bytes
}

func (c *Cluster) NotifyMsg(bytes []byte) {

}

func (c *Cluster) GetBroadcasts(overhead, limit int) [][]byte {
	return c.broadcasts.GetBroadcasts(overhead, limit)
}

func (c *Cluster) LocalState(join bool) []byte {
	c.view.update(toShardViewList(c.infoF().ShardInfoList))
	state := &clusterState{ShardView: c.view.toShuffledList()}
	b, _ := json.Marshal(state)
	return b
}

func (c *Cluster) MergeRemoteState(buf []byte, join bool) {
	remote := &clusterState{}
	_ = json.Unmarshal(buf, remote)
	c.view.update(remote.ShardView)
}
