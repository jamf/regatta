// Copyright JAMF Software, LLC

package heap

type Heap[E any] struct {
	Slice []E
	Less  func(E, E) bool
}

func New[E any](less func(E, E) bool, items ...E) *Heap[E] {
	h := &Heap[E]{
		Slice: items,
		Less:  less,
	}
	n := len(items)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
	return h
}

func (h *Heap[E]) Peek() E {
	if len(h.Slice) == 0 {
		panic("empty slice")
	}
	return h.Slice[0]
}

func (h *Heap[E]) Push(item E) {
	h.Slice = append(h.Slice, item)
	h.up(len(h.Slice) - 1)
}

func (h *Heap[E]) Pop() E {
	if len(h.Slice) == 0 {
		panic("empty slice")
	}
	n := len(h.Slice) - 1
	h.swap(0, n)
	h.down(0, n)
	return h.zpop()
}

func (h *Heap[E]) Remove(i int) E {
	n := len(h.Slice) - 1
	if n != i {
		h.swap(i, n)
		if !h.down(i, n) {
			h.up(i)
		}
	}
	return h.zpop()
}

func (h *Heap[E]) Len() int {
	return len(h.Slice)
}

func (h *Heap[E]) Fix(i int) {
	if i < 0 {
		return
	}
	if !h.down(i, len(h.Slice)) {
		h.up(i)
	}
}

func (h *Heap[E]) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(h.Slice[j], h.Slice[i]) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *Heap[E]) down(i0 int, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(h.Slice[j2], h.Slice[j1]) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(h.Slice[j], h.Slice[i]) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}

func (h *Heap[E]) swap(i, j int) {
	h.Slice[i], h.Slice[j] = h.Slice[j], h.Slice[i]
}

func (h *Heap[E]) zpop() E {
	var zero E
	e0 := h.Slice[len(h.Slice)-1]
	h.Slice[len(h.Slice)-1] = zero
	h.Slice = h.Slice[:len(h.Slice)-1]
	return e0
}
