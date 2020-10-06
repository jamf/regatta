package storage

import (
	"context"
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
func (s *SimpleStorage) Reset() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage = make(map[string][]byte)
}
