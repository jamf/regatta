package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/wandera/regatta/proto"
)

// SimpleStorage implements trivial storage for testing purposes.
type SimpleStorage struct {
	mtx     sync.RWMutex
	storage map[string][]byte
}

func (s *SimpleStorage) Range(_ context.Context, req *proto.RangeRequest) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if value, ok := s.storage[string(append(req.Table, req.Key...))]; ok {
		return value, nil
	}
	return nil, ErrNotFound
}

func (s *SimpleStorage) Put(_ context.Context, req *proto.PutRequest) (Result, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage[string(append(req.Table, req.Key...))] = req.Value
	return Result{
		Value: 1,
	}, nil
}

func (s *SimpleStorage) Delete(_ context.Context, req *proto.DeleteRangeRequest) (Result, error) {
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
func (s *SimpleStorage) Reset(ctx context.Context, req *proto.ResetRequest) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage = make(map[string][]byte)
	return nil
}

func (s *SimpleStorage) Hash(ctx context.Context, req *proto.HashRequest) (uint64, error) {
	// Encode to bin format
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(s.storage)
	if err != nil {
		return 0, err
	}
	// Compute MD5
	sum := md5.Sum(b.Bytes())
	return binary.LittleEndian.Uint64(sum[:]), nil
}
