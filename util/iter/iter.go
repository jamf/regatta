// Copyright JAMF Software, LLC

package iter

import (
	"sync"
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

func Pull[T any](seq Seq[T]) (iter func() (T, bool), stop func()) {
	next := make(chan struct{})
	yield := make(chan T)

	go func() {
		defer close(yield)

		_, ok := <-next
		if !ok {
			return
		}

		seq(func(v T) bool {
			yield <- v
			_, ok := <-next
			return ok
		})
	}()

	return func() (v T, ok bool) {
			select {
			case <-yield:
				return v, false
			case next <- struct{}{}:
				v, ok := <-yield
				return v, ok
			}
		}, sync.OnceFunc(func() {
			close(next)
			<-yield
		})
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
