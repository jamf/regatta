// Copyright JAMF Software, LLC

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationQueue(t *testing.T) {
	q := NewNotificationQueue()
	go q.Run()
	defer q.Close()

	w := q.Add(context.Background(), "foo", 1)
	require.Empty(t, w)
	q.Notify("foo", 10)
	chanelClosed(t, w)
	w = q.Add(context.Background(), "foo", 5)
	require.Empty(t, w)
	q.Notify("foo", 4)
	require.Empty(t, w)
	q.Notify("foo", 20)
	chanelClosed(t, w)

	require.Zero(t, q.Len("foo"))
}

func TestNotificationQueueTimeout(t *testing.T) {
	q := NewNotificationQueue()
	go q.Run()
	defer q.Close()

	ctx, cancel := context.WithCancel(context.TODO())
	q.Add(ctx, "foo", 1)
	q.Add(ctx, "foo", 2)
	q.Add(ctx, "foo", 3)
	w := q.Add(ctx, "foo", 4)
	cancel()

	require.Eventually(t, func() bool {
		select {
		case err := <-w:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		h, _ := q.items.Load("foo")
		return assert.Zero(t, h.Len())
	}, 5*time.Second, 100*time.Millisecond)
}

func chanelClosed(t *testing.T, w <-chan error) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-w:
			return true
		default:
			return false
		}
	}, 10*time.Millisecond, time.Millisecond)
}
