// Copyright JAMF Software, LLC

package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
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
	shardView  *shardView
	broadcasts *memberlist.TransmitLimitedQueue
	log        *zap.SugaredLogger
}

func (c *Cluster) NotifyJoin(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s joined", n)
}

func (c *Cluster) NotifyLeave(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s left", n)
}

func (c *Cluster) NotifyUpdate(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s updated", n)
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

func New(bindAddr string, advAddr string, f getClusterInfo) (*Cluster, error) {
	info := f()
	log := zap.S().Named("memberlist").WithOptions(zap.AddCallerSkip(4))
	cluster := &Cluster{log: log, infoF: f, shardView: newView()}

	mcfg := memberlist.DefaultLANConfig()
	mcfg.LogOutput = &loggerAdapter{log: log}
	mcfg.Name = strconv.FormatUint(info.NodeID, 10)
	mcfg.Events = cluster

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

	cluster.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return cluster.ml.NumMembers() },
		RetransmitMult: mcfg.RetransmitMult,
	}

	mcfg.Delegate = &delegate{
		meta: NodeMeta{
			ID:            info.NodeHostID,
			NodeID:        info.NodeID,
			RaftAddress:   info.RaftAddress,
			MemberAddress: bindAddr,
		},
		broadcasts: cluster.broadcasts,
		shardView:  cluster.shardView,
		infoF:      f,
	}

	ml, err := memberlist.Create(mcfg)
	if err != nil {
		return nil, err
	}
	cluster.ml = ml
	return cluster, err
}

func (c *Cluster) ShardInfo(id uint64) dragonboat.ShardView {
	return c.shardView.shardInfo(id)
}

func (c *Cluster) Nodes() []Node {
	members := c.ml.Members()
	ret := make([]Node, len(members))
	for i, member := range members {
		ret[i] = toNode(member)
	}
	return ret
}

type Node struct {
	memberlist.Node
	NodeMeta
}

func (n Node) String() string {
	return fmt.Sprintf("%s: {NodeId: %d, RaftAddress: %s, MemberAddress: %s}", n.ID, n.NodeID, n.RaftAddress, n.MemberAddress)
}

type NodeMeta struct {
	ID            string `json:"ID"`
	NodeID        uint64 `json:"NodeID"`
	RaftAddress   string `json:"RaftAddress"`
	MemberAddress string `json:"MemberAddress"`
}

type delegate struct {
	meta       NodeMeta
	broadcasts *memberlist.TransmitLimitedQueue
	shardView  *shardView
	infoF      getClusterInfo
}

func (c *delegate) NodeMeta(limit int) []byte {
	bytes, _ := json.Marshal(&c.meta)
	if len(bytes) > limit {
		panic("gossip meta limit exceeded")
	}
	return bytes
}

func (c *delegate) NotifyMsg(bytes []byte) {

}

func (c *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return c.broadcasts.GetBroadcasts(overhead, limit)
}

func (c *delegate) LocalState(join bool) []byte {
	c.shardView.update(toShardViewList(c.infoF().ShardInfoList))
	state := &clusterState{ShardView: c.shardView.copy()}
	b, _ := json.Marshal(state)
	return b
}

func (c *delegate) MergeRemoteState(buf []byte, join bool) {
	remote := &clusterState{}
	_ = json.Unmarshal(buf, remote)
	c.shardView.update(remote.ShardView)
}

func toNode(node *memberlist.Node) Node {
	n := Node{
		Node: memberlist.Node{
			Name:  node.Name,
			Addr:  node.Addr,
			Port:  node.Port,
			Meta:  node.Meta,
			State: node.State,
			PMin:  node.PMin,
			PMax:  node.PMax,
			PCur:  node.PCur,
			DMin:  node.DMin,
			DMax:  node.DMax,
			DCur:  node.DCur,
		},
		NodeMeta: NodeMeta{},
	}
	_ = json.Unmarshal(node.Meta, &n.NodeMeta)
	return n
}
