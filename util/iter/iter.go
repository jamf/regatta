// Copyright JAMF Software, LLC

package iter

import (
	_ "unsafe"
)

type Seq[V any] func(yield func(V) bool)

func Collect[T any](seq Seq[T]) (res []T) {
	seq(func(t T) bool {
		res = append(res, t)
		return true
	})
	return
}

func Consume[T any](seq Seq[T], fn func(T)) {
	seq(func(t T) bool {
		fn(t)
		return true
	})
}

func From[T any](in ...T) Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range in {
			if !yield(item) {
				return
			}
		}
	}
}

func First[T any](seq Seq[T]) T {
	var res T
	if seq == nil {
		return *new(T)
	}
	seq(func(t T) bool {
		res = t
		return false
	})
	return res
}

func Map[S, R any](seq Seq[S], fn func(S) R) Seq[R] {
	return func(yield func(R) bool) {
		seq(func(v S) bool {
			return yield(fn(v))
		})
	}
}

type coro struct{}

//go:linkname newcoro runtime.newcoro
func newcoro(func(*coro)) *coro

//go:linkname coroswitch runtime.coroswitch
func coroswitch(*coro)

func Pull[V any](seq Seq[V]) (next func() (V, bool), stop func()) {
	var (
		v         V
		ok        bool
		done      bool
		yieldNext bool
	)
	c := newcoro(func(c *coro) {
		yield := func(v1 V) bool {
			if done {
				return false
			}
			if !yieldNext {
				panic("iter.Pull: yield called again before next")
			}
			yieldNext = false
			v, ok = v1, true
			coroswitch(c)
			return !done
		}
		seq(yield)
		var v0 V
		v, ok = v0, false
		done = true
	})
	next = func() (v1 V, ok1 bool) {
		if done {
			return
		}
		if yieldNext {
			panic("iter.Pull: next called again before yield")
		}
		yieldNext = true
		coroswitch(c)
		return v, ok
	}
	stop = func() {
		if !done {
			done = true
			coroswitch(c)
		}
	}
	return next, stop
}

func Contains[T comparable](seq Seq[T], item T) bool {
	found := false
	seq(func(t T) bool {
		if t == item {
			found = true
			return false
		}
		return true
	})
	return found
}
