package util

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	m   map[K]V
	mtx sync.RWMutex
}

func (s *SyncMap[K, V]) Load(key K) (V, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.m == nil {
		return *new(V), false
	}
	v, ok := s.m[key]
	return v, ok
}

func (s *SyncMap[K, V]) Store(key K, val V) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.m == nil {
		s.m = make(map[K]V)
	}
	s.m[key] = val
}

func (s *SyncMap[K, V]) ComputeIfAbsent(key K, valFunc func(K) V) V {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.m == nil {
		s.m = make(map[K]V)
	}
	v, ok := s.m[key]
	if !ok {
		v = valFunc(key)
		s.m[key] = v
	}
	return v
}

func (s *SyncMap[K, V]) Delete(key K) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.m, key)
}
