package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"hash/fnv"
	"sync"

	"github.com/wandera/regatta/proto"
)

// Mock implements trivial storage for testing purposes.
type Mock struct {
	mtx     sync.RWMutex
	storage map[string][]byte
}

func (s *Mock) Range(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if value, ok := s.storage[string(append(req.Table, req.Key...))]; ok {
		return &proto.RangeResponse{
			Kvs: []*proto.KeyValue{
				{
					Key:   req.Key,
					Value: value,
				},
			},
			Count: 1,
		}, nil
	}
	return nil, ErrNotFound
}

func (s *Mock) Put(_ context.Context, req *proto.PutRequest) (Result, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage[string(append(req.Table, req.Key...))] = req.Value
	return Result{
		Value: 1,
	}, nil
}

func (s *Mock) Delete(_ context.Context, req *proto.DeleteRangeRequest) (Result, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	key := string(append(req.Table, req.Key...))
	if _, ok := s.storage[key]; ok {
		delete(s.storage, key)
		return Result{
			Value: 1,
		}, nil
	}
	return Result{
		Value: 0,
	}, ErrNotFound
}

// Reset method resets storage.
func (s *Mock) Reset(ctx context.Context, req *proto.ResetRequest) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage = make(map[string][]byte)
	return nil
}

func (s *Mock) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	// Encode to bin format
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(s.storage)
	if err != nil {
		return nil, err
	}
	// Compute Hash
	hash64 := fnv.New64()
	_, err = hash64.Write(b.Bytes())
	if err != nil {
		return nil, err
	}
	return &proto.HashResponse{Hash: hash64.Sum64()}, nil
}
