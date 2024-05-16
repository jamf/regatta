// Copyright JAMF Software, LLC

package util

import (
	"sync"

	"github.com/jamf/regatta/util/iter"
)

type SyncMap[K comparable, V any] struct {
	m           map[K]V
	mtx         sync.RWMutex
	defaultFunc func(K) V
}

func NewSyncMap[K comparable, V any](defaulter func(K) V) *SyncMap[K, V] {
	return &SyncMap[K, V]{m: make(map[K]V), defaultFunc: defaulter}
}

type SyncMapPair[K comparable, V any] struct {
	Key K
	Val V
}

func (s *SyncMap[K, V]) Pairs() iter.Seq[SyncMapPair[K, V]] {
	return func(yield func(SyncMapPair[K, V]) bool) {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		for k, v := range s.m {
			if !yield(SyncMapPair[K, V]{Key: k, Val: v}) {
				break
			}
		}
	}
}

func (s *SyncMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		for k := range s.m {
			if !yield(k) {
				break
			}
		}
	}
}

func (s *SyncMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		for _, v := range s.m {
			if !yield(v) {
				break
			}
		}
	}
}

func (s *SyncMap[K, V]) Load(key K) (V, bool) {
	s.mtx.RLock()
	v, ok := s.m[key]
	if !ok && s.defaultFunc != nil {
		s.mtx.RUnlock()
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.m[key] = s.defaultFunc(key)
		return s.m[key], true
	}
	s.mtx.RUnlock()
	return v, ok
}

func (s *SyncMap[K, V]) Store(key K, val V) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.m[key] = val
}

func (s *SyncMap[K, V]) ComputeIfAbsent(key K, valFunc func(K) V) V {
	s.mtx.Lock()
	defer s.mtx.Unlock()
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
