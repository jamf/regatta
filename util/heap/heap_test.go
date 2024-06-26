// Copyright JAMF Software, LLC

package heap

import (
	"math/rand/v2"
	"testing"
)

func newHeap() *Heap[int] {
	return &Heap[int]{
		Less: func(i, j int) bool {
			return i < j
		},
	}
}

func verify[T any](t *testing.T, h *Heap[T], i int) {
	t.Helper()
	n := h.Len()
	j1 := 2*i + 1
	j2 := 2*i + 2
	if j1 < n {
		if h.Less(h.Slice[j1], h.Slice[i]) {
			t.Errorf("heap invariant invalidated [%d] = %v > [%d] = %v", i, h.Slice[i], j1, h.Slice[j1])
			return
		}
		verify(t, h, j1)
	}
	if j2 < n {
		if h.Less(h.Slice[j2], h.Slice[i]) {
			t.Errorf("heap invariant invalidated [%d] = %v > [%d] = %v", i, h.Slice[i], j1, h.Slice[j2])
			return
		}
		verify(t, h, j2)
	}
}

func TestInit0(t *testing.T) {
	h := newHeap()
	for i := 20; i > 0; i-- {
		h.Push(0) // all elements are the same
	}
	verify(t, h, 0)

	for i := 1; h.Len() > 0; i++ {
		x := h.Pop()
		verify(t, h, 0)
		if x != 0 {
			t.Errorf("%d.th pop got %d; want %d", i, x, 0)
		}
	}
}

func TestInit1(t *testing.T) {
	h := newHeap()
	for i := 20; i > 0; i-- {
		h.Push(i) // all elements are different
	}
	verify(t, h, 0)

	for i := 1; h.Len() > 0; i++ {
		x := h.Pop()
		verify(t, h, 0)
		if x != i {
			t.Errorf("%d.th pop got %d; want %d", i, x, i)
		}
	}
}

func Test(t *testing.T) {
	h := newHeap()
	verify(t, h, 0)

	for i := 20; i > 10; i-- {
		h.Push(i)
	}
	verify(t, h, 0)

	for i := 10; i > 0; i-- {
		h.Push(i)
		verify(t, h, 0)
	}

	for i := 1; h.Len() > 0; i++ {
		x := h.Pop()
		if i < 20 {
			h.Push(20 + i)
		}
		verify(t, h, 0)
		if x != i {
			t.Errorf("%d.th pop got %d; want %d", i, x, i)
		}
	}
}

func TestRemove0(t *testing.T) {
	h := newHeap()
	for i := 0; i < 10; i++ {
		h.Push(i)
	}
	verify(t, h, 0)

	for h.Len() > 0 {
		i := h.Len() - 1
		x := h.Remove(i)
		if x != i {
			t.Errorf("Remove(%d) got %d; want %d", i, x, i)
		}
		verify(t, h, 0)
	}
}

func TestRemove1(t *testing.T) {
	h := newHeap()
	for i := 0; i < 10; i++ {
		h.Push(i)
	}
	verify(t, h, 0)

	for i := 0; h.Len() > 0; i++ {
		x := h.Remove(0)
		if x != i {
			t.Errorf("Remove(0) got %d; want %d", x, i)
		}
		verify(t, h, 0)
	}
}

func TestRemove2(t *testing.T) {
	N := 10

	h := newHeap()
	for i := 0; i < N; i++ {
		h.Push(i)
	}
	verify(t, h, 0)

	m := make(map[int]bool)
	for h.Len() > 0 {
		m[h.Remove((h.Len()-1)/2)] = true
		verify(t, h, 0)
	}

	if len(m) != N {
		t.Errorf("len(m) = %d; want %d", len(m), N)
	}
	for i := 0; i < len(m); i++ {
		if !m[i] {
			t.Errorf("m[%d] doesn't exist", i)
		}
	}
}

func BenchmarkDup(b *testing.B) {
	const n = 10000
	h := newHeap()
	h.Slice = make([]int, 0, n)
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			h.Push(0) // all elements are the same
		}
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

func TestFix(t *testing.T) {
	h := newHeap()
	verify(t, h, 0)

	for i := 200; i > 0; i -= 10 {
		h.Push(i)
	}
	verify(t, h, 0)

	if h.Slice[0] != 10 {
		t.Fatalf("Expected head to be 10, was %d", h.Slice[0])
	}
	h.Slice[0] = 210
	h.Fix(0)
	verify(t, h, 0)

	for i := 100; i > 0; i-- {
		elem := rand.IntN(h.Len())
		if i&1 == 0 {
			h.Slice[elem] *= 2
		} else {
			h.Slice[elem] /= 2
		}
		h.Fix(elem)
		verify(t, h, 0)
	}
}
