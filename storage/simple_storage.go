package storage

import (
	"errors"
	"sync"
)

// SimpleStorage implements trivial storage for testing purposes.
type SimpleStorage struct {
	mtx     sync.RWMutex
	storage map[string][]byte
}

// ErrNotFound returned when the key is not found.
var ErrNotFound error = errors.New("Key not found")

// Reset method resets storage.
func (s *SimpleStorage) Reset() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage = make(map[string][]byte)
}

// Put method writes `value` to `table` under `key`.
func (s *SimpleStorage) Put(table []byte, key []byte, value []byte) ([]byte, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage[string(append(table, key...))] = value
	return value, nil
}

// Get method returns `value` from `table` under `key`.
func (s *SimpleStorage) Get(table []byte, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if value, ok := s.storage[string(append(table, key...))]; ok {
		return value, nil
	}
	return nil, ErrNotFound
}

// Delete method drops `key` from `table`.
func (s *SimpleStorage) Delete(table []byte, key []byte) ([]byte, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if value, ok := s.storage[string(append(table, key...))]; ok {
		delete(s.storage, string(append(table, key...)))
		return value, nil
	}
	return nil, ErrNotFound
}

// PutDummyData method fills some dummy data.
func (s *SimpleStorage) PutDummyData() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.storage["table_1key_1"] = []byte("table_1value_1")
	s.storage["table_1key_2"] = []byte("table_1value_2")
	s.storage["table_2key_1"] = []byte("table_2value_1")
	s.storage["table_2key_2"] = []byte("table_2value_2")
}
