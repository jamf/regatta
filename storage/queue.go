// Copyright JAMF Software, LLC

package storage

import (
	"cmp"
	"context"
	"time"

	"github.com/jamf/regatta/util"
	"github.com/jamf/regatta/util/heap"
	"github.com/jamf/regatta/util/iter"
)

type item struct {
	ctx      context.Context
	table    string
	revision uint64
	waitCh   chan error
}

func (i *item) less(other *item) bool {
	return cmp.Less(i.revision, other.revision)
}

type reqlen struct {
	table  string
	waitCh chan int
}

type notification struct {
	table    string
	revision uint64
}

type IndexNotificationQueue struct {
	items  *util.SyncMap[string, *heap.Heap[*item]]
	add    chan *item
	notif  chan notification
	len    chan reqlen
	closed chan struct{}
}

func NewNotificationQueue() *IndexNotificationQueue {
	return &IndexNotificationQueue{
		add:    make(chan *item),
		notif:  make(chan notification),
		closed: make(chan struct{}),
		len:    make(chan reqlen),
		items: util.NewSyncMap(func(_ string) *heap.Heap[*item] {
			return heap.New((*item).less)
		}),
	}
}

func (q *IndexNotificationQueue) Run() {
	gc := time.NewTicker(time.Second)
	defer gc.Stop()
	for {
		select {
		case <-q.closed:
			return
		case <-gc.C:
			iter.Consume(q.items.Values(), func(h *heap.Heap[*item]) {
				l := h.Len()
				for i := 0; i < l; i++ {
					elem := h.Slice[i]
					if elem.ctx.Err() != nil {
						// Reorder
						elem.revision = 0
						elem.waitCh <- elem.ctx.Err()
					}
				}
				h.Fix(0)
				for i := 0; i < l; i++ {
					elem := h.Peek()
					if elem.revision == 0 {
						h.Pop()
					} else {
						break
					}
				}
			})
		case it := <-q.add:
			h, _ := q.items.Load(it.table)
			h.Push(it)
		case n := <-q.notif:
			h, _ := q.items.Load(n.table)
			l := h.Len()
			for i := 0; i < l; i++ {
				elem := h.Peek()
				if elem.ctx.Err() != nil {
					elem.waitCh <- elem.ctx.Err()
					h.Pop()
				} else if elem.revision <= n.revision {
					close(elem.waitCh)
					h.Pop()
				} else {
					break
				}
			}
		case req := <-q.len:
			h, _ := q.items.Load(req.table)
			req.waitCh <- h.Len()
		}
	}
}

func (q *IndexNotificationQueue) Len(table string) int {
	respCh := make(chan int)
	q.len <- reqlen{table: table, waitCh: respCh}
	return <-respCh
}

func (q *IndexNotificationQueue) Notify(table string, revision uint64) {
	q.notif <- notification{table: table, revision: revision}
}

func (q *IndexNotificationQueue) Close() error {
	close(q.closed)
	return nil
}

func (q *IndexNotificationQueue) Add(ctx context.Context, table string, revision uint64) <-chan error {
	ch := make(chan error, 1)
	q.add <- &item{
		ctx:      ctx,
		table:    table,
		revision: revision,
		waitCh:   ch,
	}
	return ch
}
