package storage

import (
	"encoding/hex"
	"hash"
	"sync"

	"github.com/minio/highwayhash"
	"go.uber.org/zap"
)

const goldenKey = "0201020504F506070806AA0B0C0D0E0AA0E0D0C0B0A0B0807090504030205000"

// Partitioner calculates ClusterID for given key.
type Partitioner interface {
	ClusterID(key []byte) uint64
	Capacity() uint64
}

// StaticPartitioner is the Partitioner that always return 1.
type StaticPartitioner struct {
}

// ClusterID returns the partition ID for the specified raft cluster.
func (p *StaticPartitioner) ClusterID(key []byte) uint64 {
	return 1
}

// Capacity of a partitioner.
func (p *StaticPartitioner) Capacity() uint64 {
	return 1
}

// HashPartitioner is the Partitioner with fixed capacity and naive
// partitioning strategy.
type HashPartitioner struct {
	capacity uint64
	log      *zap.Logger
	hash     hash.Hash64
	mtx      sync.Mutex
}

// NewHashPartitioner creates a new HashPartitioner instance.
func NewHashPartitioner(capacity uint64) *HashPartitioner {
	log := zap.L().Named("partitioner")
	if capacity == 0 {
		log.Panic("capacity cannot be 0")
	}
	key, err := hex.DecodeString(goldenKey)
	if err != nil {
		log.Panic("cannot instantiate hash function")
	}
	h, err := highwayhash.New64(key)
	if err != nil {
		log.Panic("cannot instantiate hash function")
	}
	return &HashPartitioner{
		capacity: capacity,
		log:      log,
		hash:     h,
	}
}

// ClusterID returns the partition ID for the specified raft cluster.
func (p *HashPartitioner) ClusterID(key []byte) uint64 {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.hash.Reset()
	_, err := p.hash.Write(key)
	if err != nil {
		p.log.Panic("failed to compute hash")
	}
	return p.hash.Sum64()%p.capacity + 1
}

// Capacity of a partitioner.
func (p *HashPartitioner) Capacity() uint64 {
	return p.capacity
}
