package util

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (s *SyncMap[K, V]) Load(key K) (V, bool) {
	v, ok := s.m.Load(key)
	if ok {
		return v.(V), ok
	}
	return *new(V), ok
}

func (s *SyncMap[K, V]) Store(key K, val V) {
	s.m.Store(key, val)
}

func (s *SyncMap[K, V]) LoadOrStore(key K, val V) (V, bool) {
	v, ok := s.m.LoadOrStore(key, val)
	if !ok {
		return v.(V), ok
	}
	return *new(V), ok
}

func (s *SyncMap[K, V]) LoadAndDelete(key K) (V, bool) {
	v, ok := s.m.LoadAndDelete(key)
	if ok {
		return v.(V), ok
	}
	return *new(V), ok
}

func (s *SyncMap[K, V]) Delete(key K) {
	s.m.Delete(key)
}

func (s *SyncMap[K, V]) Range(f func(K, V) bool) {
	s.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
