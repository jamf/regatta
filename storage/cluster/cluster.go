// Copyright JAMF Software, LLC

package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"go.uber.org/zap"
)

type Message struct {
	Key     string `json:"key"`
	Payload []byte `json:"payload"`
}

func (m Message) Invalidates(b memberlist.Broadcast) bool {
	if o, ok := b.(memberlist.NamedBroadcast); ok {
		return m.Name() == o.Name()
	}
	return false
}

func (m Message) Name() string {
	return m.Key
}

func (m Message) Message() []byte {
	b, _ := json.Marshal(&m)
	return b
}

func (m Message) Finished() {
}

type clusterState struct {
	ShardView []dragonboat.ShardView `json:"shard_view"`
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

type listener struct {
	ch   chan Message
	stop chan struct{}
	f    func(Message)
}

func (l *listener) handle() {
	for {
		select {
		case m := <-l.ch:
			l.f(m)
		case <-l.stop:
			return
		}
	}
}

type Cluster struct {
	ml           *memberlist.Memberlist
	infoF        getClusterInfo
	shardView    *shardView
	broadcasts   *memberlist.TransmitLimitedQueue
	log          *zap.SugaredLogger
	keyListeners struct {
		mu        sync.RWMutex
		listeners map[string]*listener
	}
	prefixListeners struct {
		mu        sync.RWMutex
		listeners map[string]*listener
	}
	msgs chan Message
	stop chan struct{}
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

func (c *Cluster) LocalNode() Node {
	return toNode(c.ml.LocalNode())
}

func (c *Cluster) SendTo(n Node, m Message) error {
	return c.ml.SendReliable(&n.Node, m.Message())
}

func (c *Cluster) Broadcast(m Message) {
	c.broadcasts.QueueBroadcast(m)
}

func (c *Cluster) Start(join []string) (int, error) {
	go c.dispatch()
	return c.ml.Join(join)
}

func (c *Cluster) dispatch() {
	for {
		select {
		case msg := <-c.msgs:
			c.keyListeners.mu.RLock()
			if l, ok := c.keyListeners.listeners[msg.Key]; ok {
				l.ch <- msg
			}
			c.keyListeners.mu.RUnlock()

			c.prefixListeners.mu.RLock()
			for prefix, l := range c.prefixListeners.listeners {
				if strings.HasPrefix(msg.Key, prefix) {
					l.ch <- msg
				}
			}
			c.prefixListeners.mu.RUnlock()
		case <-c.stop:
			return
		}
	}
}

func (c *Cluster) WatchKey(key string, f func(message Message)) {
	c.keyListeners.mu.Lock()
	defer c.keyListeners.mu.Unlock()
	if c.keyListeners.listeners == nil {
		c.keyListeners.listeners = make(map[string]*listener)
	}
	ls := &listener{ch: make(chan Message, 1), stop: c.stop, f: f}
	c.keyListeners.listeners[key] = ls
	go ls.handle()
}

func (c *Cluster) WatchPrefix(key string, f func(message Message)) {
	c.prefixListeners.mu.Lock()
	defer c.prefixListeners.mu.Unlock()
	if c.prefixListeners.listeners == nil {
		c.prefixListeners.listeners = make(map[string]*listener)
	}
	ls := &listener{ch: make(chan Message, 1), stop: c.stop, f: f}
	c.prefixListeners.listeners[key] = ls
	go ls.handle()
}

func (c *Cluster) Close() error {
	waitTimeout := time.Now().Add(10 * time.Second)
	for c.broadcasts.NumQueued() > 0 && c.ml.NumMembers() > 1 && time.Now().Before(waitTimeout) {
		time.Sleep(250 * time.Millisecond)
	}

	if cnt := c.broadcasts.NumQueued(); cnt > 0 {
		c.log.Warnf("broadcast messages left in queue %d", cnt)
	}

	close(c.stop)

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
	cluster := &Cluster{log: log, infoF: f, shardView: newView(), stop: make(chan struct{}), msgs: make(chan Message, 1)}

	mcfg := memberlist.DefaultLANConfig()
	mcfg.LogOutput = &loggerAdapter{log: log}
	mcfg.Name = fmt.Sprintf("%s/%d", info.NodeHostID, info.NodeID)
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
		msgs:       cluster.msgs,
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
	return fmt.Sprintf("%s: {node_id: %d, raft_address: %s, member_address: %s}", n.ID, n.NodeID, n.RaftAddress, n.MemberAddress)
}

type NodeMeta struct {
	ID            string `json:"id"`
	NodeID        uint64 `json:"node_id"`
	RaftAddress   string `json:"raft_address"`
	MemberAddress string `json:"member_address"`
}

type delegate struct {
	meta       NodeMeta
	msgs       chan Message
	broadcasts *memberlist.TransmitLimitedQueue
	shardView  *shardView
	infoF      getClusterInfo
}

func (c *delegate) NodeMeta(_ int) []byte {
	bytes, _ := json.Marshal(&c.meta)
	return bytes
}

func (c *delegate) NotifyMsg(bytes []byte) {
	m := Message{}
	if err := json.Unmarshal(bytes, &m); err == nil {
		c.msgs <- m
	}
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
