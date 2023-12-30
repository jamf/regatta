// Copyright JAMF Software, LLC

package iter

import (
	"sync"
)

type Seq[V any] func(yield func(V) bool)

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
