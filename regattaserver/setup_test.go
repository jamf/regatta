package regattaserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"hash/fnv"
	"os"
	"sync"
	"testing"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
)

func setup() {
	s := MockStorage{}
	_ = s.Reset(context.TODO(), &proto.ResetRequest{})

	kv = KVServer{
		Storage: &s,
	}

	ms = MaintenanceServer{
		Storage: &s,
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

// MockStorage implements trivial storage for testing purposes.
type MockStorage struct {
	mtx     sync.RWMutex
	storage map[string][]byte
}

func (s *MockStorage) Range(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if len(req.Table) == 0 {
		return nil, storage.ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return nil, storage.ErrEmptyKey
	}

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
	return nil, storage.ErrNotFound
}

func (s *MockStorage) Put(_ context.Context, req *proto.PutRequest) (storage.Result, error) {
	if len(req.Table) == 0 {
		return storage.Result{}, storage.ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return storage.Result{}, storage.ErrEmptyKey
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage[string(append(req.Table, req.Key...))] = req.Value
	return storage.Result{
		Value: 1,
	}, nil
}

func (s *MockStorage) Delete(_ context.Context, req *proto.DeleteRangeRequest) (storage.Result, error) {
	if len(req.Table) == 0 {
		return storage.Result{}, storage.ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return storage.Result{}, storage.ErrEmptyKey
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	key := string(append(req.Table, req.Key...))
	if _, ok := s.storage[key]; ok {
		delete(s.storage, key)
		return storage.Result{
			Value: 1,
		}, nil
	}
	return storage.Result{
		Value: 0,
	}, storage.ErrNotFound
}

// Reset method resets storage.
func (s *MockStorage) Reset(ctx context.Context, req *proto.ResetRequest) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage = make(map[string][]byte)
	return nil
}

func (s *MockStorage) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
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
