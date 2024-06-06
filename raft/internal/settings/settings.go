// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package settings

const (
	// EntryNonCmdFieldsSize defines the upper limit of the non-cmd field
	// length in pb.Entry.
	EntryNonCmdFieldsSize = 16 * 8
	// LargeEntitySize defines what is considered as a large entity for per node
	// entities.
	LargeEntitySize uint64 = 64 * 1024 * 1024
	// MaxEntrySize defines the max total entry size that can be included in
	// the Replicate message.
	MaxEntrySize uint64 = MaxMessageBatchSize
	// InMemEntrySliceSize defines the maximum length of the in memory entry
	// slice.
	InMemEntrySliceSize uint64 = 512
	// MinEntrySliceFreeSize defines the minimum length of the free in memory
	// entry slice. A new entry slice of length InMemEntrySliceSize will be
	// allocated once the free entry size in the current slice is less than
	// MinEntrySliceFreeSize.
	MinEntrySliceFreeSize uint64 = 96
	// InMemGCTimeout defines how often dragonboat collects partial object.
	// It is defined in terms of number of ticks.
	InMemGCTimeout uint64 = 100
	// MaxApplyEntrySize defines the max size of entries to apply.
	MaxApplyEntrySize uint64 = 64 * 1024 * 1024

	//
	// Multiraft
	//

	// PendingProposalShards defines the number of shards for the pending
	// proposal data structure.
	PendingProposalShards uint64 = 16
	// SyncTaskInterval defines the interval in millisecond of periodic sync
	// state machine task.
	SyncTaskInterval uint64 = 180000
	// IncomingReadIndexQueueLength defines the number of pending read index
	// requests allowed for each raft group.
	IncomingReadIndexQueueLength uint64 = 4096
	// IncomingProposalQueueLength defines the number of pending proposals
	// allowed for each raft group.
	IncomingProposalQueueLength uint64 = 2048
	// ReceiveQueueLength is the length of the receive queue on each node.
	ReceiveQueueLength uint64 = 1024
	// SnapshotStatusPushDelayMS is the number of millisecond delays we impose
	// before pushing the snapshot results to raft node.
	SnapshotStatusPushDelayMS uint64 = 1000
	// TaskQueueTargetLength defined the target length of each node's taskQ.
	// Dragonboat tries to make sure the queue is no longer than this target
	// length.
	TaskQueueTargetLength uint64 = 64
	// TaskQueueInitialCap defines the initial capcity of a task queue.
	TaskQueueInitialCap uint64 = 24
	// NodeHostRequestStatePoolShards defines the number of sync pools.
	NodeHostRequestStatePoolShards uint64 = 8
	// LazyFreeCycle defines how often should entry queue and message queue
	// to be freed.
	LazyFreeCycle uint64 = 1

	//
	// step engine
	//

	// TaskBatchSize defines the length of the committed batch slice.
	TaskBatchSize uint64 = 512
	// NodeReloadMillisecond defines how often step engine should reload
	// nodes, it is defined in number of millisecond.
	NodeReloadMillisecond uint64 = 200
	// CloseWorkerTimedWaitSecond is the number of seconds allowed for the
	// close worker to run cleanups before exit.
	CloseWorkerTimedWaitSecond uint64 = 5

	//
	// transport
	//

	// GetConnectedTimeoutSecond is the default timeout value in second when
	// trying to connect to a gRPC based server.
	GetConnectedTimeoutSecond uint64 = 5
	// MaxSnapshotConnections defines the max number of concurrent outgoing
	// snapshot connections.
	MaxSnapshotConnections uint64 = 64
	// MaxConcurrentStreamingSnapshot defines the max number of concurrent
	// incoming snapshot streams.
	MaxConcurrentStreamingSnapshot uint64 = 128
	// SendQueueLength is the length of the send queue used to hold messages
	// exchanged between nodehosts. You may need to increase this value when
	// you want to host large number nodes per nodehost.
	SendQueueLength uint64 = 1024 * 2
	// StreamConnections defines how many connections to use for each remote
	// nodehost whene exchanging raft messages
	StreamConnections uint64 = 4
	// PerConnectionSendBufSize is the size of the per connection buffer used for receiving incoming messages.
	PerConnectionSendBufSize uint64 = 2 * 1024 * 1024
	// PerConnectionRecvBufSize is the size of the recv buffer size.
	PerConnectionRecvBufSize uint64 = 2 * 1024 * 1024
	// SnapshotGCTick defines the number of ticks between two snapshot GC
	// operations.
	SnapshotGCTick uint64 = 30
	// SnapshotChunkTimeoutTick defines the max time allowed to receive
	// a snapshot.
	SnapshotChunkTimeoutTick uint64 = 900
)

const (
	LRUMaxSessionCount  = 4096
	LogDBEntryBatchSize = 48
)

// BlockFileMagicNumber is the magic number used in block based snapshot files.
var BlockFileMagicNumber = []byte{0x3F, 0x5B, 0xCB, 0xF1, 0xFA, 0xBA, 0x81, 0x9F}

const (
	//
	// RSM
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	//

	// UnmanagedDeploymentID is the special deployment ID value used when no user
	// deployment ID is specified.
	UnmanagedDeploymentID uint64 = 1
	// MaxMessageBatchSize is the max size for a single message batch sent between
	// nodehosts.
	MaxMessageBatchSize uint64 = LargeEntitySize
	// SnapshotChunkSize is the snapshot chunk size.
	SnapshotChunkSize uint64 = 2 * 1024 * 1024
)
