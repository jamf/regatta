// Copyright JAMF Software, LLC

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftio"
	"github.com/jamf/regatta/storage/cluster/dns"
	"go.uber.org/zap"
)

// Message sent between the members of the memberlist.
type Message struct {
	Key     string `json:"key"`
	Payload []byte `json:"payload"`
}

// Invalidates checks if enqueuing the current broadcast
// invalidates a previous broadcast.
func (m Message) Invalidates(b memberlist.Broadcast) bool {
	if o, ok := b.(memberlist.NamedBroadcast); ok {
		return m.Name() == o.Name()
	}
	return false
}

// Name is a unique identity of this broadcast message.
func (m Message) Name() string {
	return m.Key
}

// Message returns a byte representation of the message.
func (m Message) Message() []byte {
	b, _ := json.Marshal(&m)
	return b
}

// Finished is invoked when the message will no longer
// be broadcast, either due to invalidation or to the
// transmit limit being reached.
func (m Message) Finished() {
}

type resolver interface {
	Resolve(ctx context.Context, addrs []string) error
	Addresses() []string
}

type clusterState struct {
	ShardView []raft.ShardView `json:"shard_view"`
}

// Info carries Raft-related information to the particular NodeHost in the cluster.
type Info struct {
	// NodeHostID is the unique identifier of the NodeHost instance.
	NodeHostID string
	// NodeID is the ID of this replica.
	NodeID uint64
	// RaftAddress is the public address of the NodeHost used for exchanging Raft
	// messages, snapshots and other metadata with other NodeHost instances.
	RaftAddress string
	// ClientAddress is the public address of the Regatta host used for client requests.
	ClientAddress string
	// ShardInfo is a list of all Raft shards managed by the NodeHost
	ShardInfoList []raft.ShardInfo
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost.
	LogInfo []raftio.NodeInfo
}

// listener is responsible for processing received messages with a given key or prefix.
type listener struct {
	ch   chan Message
	stop chan struct{}
	// f is called by the listener when receiving a message.
	f func(Message)
}

// handle waits for a message and processes it. It blocks
// until receiving the stop message.
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

// listenerStore is a convenience data structure for storing a map of listeners
// wrapped by a sync.RWMutex.
type listenerStore struct {
	mu        sync.RWMutex
	listeners map[string]*listener
}

type getClusterInfo func() Info

// Cluster holds information about the memberlist cluster and active listeners.
type Cluster struct {
	name            string
	ml              *memberlist.Memberlist
	infoF           getClusterInfo
	shardView       *shardView
	broadcasts      *memberlist.TransmitLimitedQueue
	log             *zap.SugaredLogger
	keyListeners    listenerStore
	prefixListeners listenerStore
	msgs            chan Message
	not             chan struct{}
	stop            chan struct{}
	resolver        resolver
	addrs           []string
}

func (c *Cluster) Notify() {
	c.shardView.update(toShardViewList(c.infoF().ShardInfoList))
	select {
	case c.not <- struct{}{}:
	default:
	}
}

func (c *Cluster) NotifyJoin(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s joined", n)
	c.shardView.update(toShardViewList(c.infoF().ShardInfoList))
}

func (c *Cluster) NotifyLeave(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s left", n)
	c.shardView.update(toShardViewList(c.infoF().ShardInfoList))
}

func (c *Cluster) NotifyUpdate(node *memberlist.Node) {
	n := toNode(node)
	c.log.Infof("%s updated", n)
	c.shardView.update(toShardViewList(c.infoF().ShardInfoList))
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

func (c *Cluster) Start(join []string) {
	go c.dispatch()
	go c.discover()
	n, err := c.ml.Join(c.discoverMembers(join))
	if err != nil {
		c.log.Warnf("failed to fast join cluster")
	} else {
		c.log.Infof("fast joined %d nodes", n)
	}
	go c.notify()
}

// dispatch receives a message and forwards it to the listener responsible for the given
// message key or prefix. If no such listener exists, the message is ignored.
// The method blocks until it receives a stop message.
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

func (c *Cluster) notify() {
	for {
		select {
		case <-c.not:
			if err := c.ml.UpdateNode(5 * time.Second); err != nil {
				c.log.Warnf("unable to update node: %v", err)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *Cluster) discover() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err := c.ml.Join(c.discoverMembers(c.addrs))
			if err != nil {
				return
			}
		case <-c.stop:
			return
		}
	}
}

func (c *Cluster) discoverMembers(members []string) []string {
	if len(members) == 0 {
		return nil
	}
	var ms, resolve []string
	for _, member := range members {
		if dns.IsDynamicNode(member) {
			resolve = append(resolve, member)
		} else {
			// No DNS record to lookup, just append member.
			ms = append(ms, member)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.resolver.Resolve(ctx, resolve)
	if err != nil {
		c.log.Warnf("failed to resolve members '%s': %v", strings.Join(resolve, ","), err)
	}
	return append(ms, c.resolver.Addresses()...)
}

// WatchKey sets up a background listener for the given key. Anytime a message with the
// key is received, the supplied function f is called.
func (c *Cluster) WatchKey(key string, f func(message Message)) {
	c.keyListeners.mu.Lock()
	defer c.keyListeners.mu.Unlock()
	ls := &listener{ch: make(chan Message, 1), stop: c.stop, f: f}
	c.keyListeners.listeners[key] = ls
	go ls.handle()
}

// WatchPrefix sets up a background listener for the given key prefix. Anytime a message with a
// key matching the specified prefix is received, the supplied function f is called.
func (c *Cluster) WatchPrefix(key string, f func(message Message)) {
	c.prefixListeners.mu.Lock()
	defer c.prefixListeners.mu.Unlock()
	ls := &listener{ch: make(chan Message, 1), stop: c.stop, f: f}
	c.prefixListeners.listeners[key] = ls
	go ls.handle()
}

// Close gracefully disconnects the node from the memberlist cluster.
// It tries to wait to broadcast currently pending outgoing messages before leaving.
func (c *Cluster) Close() error {
	waitTimeout := time.Now().Add(10 * time.Second)
	for c.broadcasts.NumQueued() > 0 && c.ml.NumMembers() > 1 && time.Now().Before(waitTimeout) {
		time.Sleep(250 * time.Millisecond)
	}

	if cnt := c.broadcasts.NumQueued(); cnt > 0 {
		c.log.Warnf("broadcast messages left in queue %d", cnt)
	}

	if err := c.ml.Leave(20 * time.Second); err != nil {
		return err
	}

	close(c.stop)

	if err := c.ml.Shutdown(); err != nil {
		return err
	}
	return nil
}

// New configures and creates a new memberlist. To connect the node to the cluster, see (*Cluster).Start.
func New(bindAddr, advAddr, clusterName, nodeName string, f getClusterInfo) (*Cluster, error) {
	info := f()
	log := zap.S().Named("memberlist")
	cluster := &Cluster{
		name:            clusterName,
		log:             log,
		infoF:           f,
		shardView:       newView(),
		not:             make(chan struct{}, 1),
		stop:            make(chan struct{}),
		msgs:            make(chan Message, 1),
		keyListeners:    listenerStore{listeners: map[string]*listener{}},
		prefixListeners: listenerStore{listeners: map[string]*listener{}},
		resolver:        dns.NewResolver(log.Named("dns")),
	}

	mcfg := memberlist.DefaultLANConfig()
	mcfg.LogOutput = &loggerAdapter{log: log.WithOptions(zap.AddCallerSkip(4))}
	mcfg.Label = clusterName
	mcfg.Name = nodeName
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
			ClientAddress: info.ClientAddress,
			MemberAddress: bindAddr,
		},
		broadcasts: cluster.broadcasts,
		shardView:  cluster.shardView,
		msgs:       cluster.msgs,
		infoF:      f,
	}
	// init view
	ml, err := memberlist.Create(mcfg)
	if err != nil {
		return nil, err
	}
	cluster.ml = ml
	return cluster, err
}

// ShardInfo retrieves a record representing the state of the Raft shard.
func (c *Cluster) ShardInfo(id uint64) raft.ShardView {
	return c.shardView.shardInfo(id)
}

// Nodes returns a list of all live nodes in the memberlist.
func (c *Cluster) Nodes() []Node {
	members := c.ml.Members()
	ret := make([]Node, len(members))
	for i, member := range members {
		ret[i] = toNode(member)
	}
	return ret
}

// Name returns human-readable cluster name.
func (c *Cluster) Name() string {
	return c.name
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
	ClientAddress string `json:"client_address"`
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
