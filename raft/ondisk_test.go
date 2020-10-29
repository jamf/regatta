package raft

import (
	"io"
	"sync"
	"testing"

	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
)

func TestKVStateMachine_Snapshot(t *testing.T) {
	tests := []struct {
		name        string
		producingSM statemachine.IOnDiskStateMachine
		receivingSM statemachine.IOnDiskStateMachine
	}{
		{
			"Pebble -> Pebble",
			filledPebbleSM(),
			emptyPebbleSM(),
		},
		{
			"Badger -> Badger",
			filledBadgerSM(),
			emptyBadgerSM(),
		},
		{
			"Pebble -> Badger",
			filledPebbleSM(),
			emptyBadgerSM(),
		},
		{
			"Badger -> Pebble",
			filledBadgerSM(),
			emptyPebbleSM(),
		},
	}
	for _, test := range tests {
		t.Log("Applying snapshot to the empty DB should produce the same hash")
		t.Run(test.name, func(t *testing.T) {
			r := require.New(t)
			p := filledBadgerSM()
			defer p.Close()

			want, err := p.GetHash()
			r.NoError(err)

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			pr, pw := io.Pipe()
			ep := emptyBadgerSM()
			defer ep.Close()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				t.Log("Save snapshot routine started")
				err := p.SaveSnapshot(snp, pw, nil)
				r.NoError(err)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				t.Log("Recover from snapshot routine started")
				err := ep.RecoverFromSnapshot(pr, nil)
				r.NoError(err)
			}()

			wg.Wait()
			t.Log("Recovery finished")
			got, err := ep.GetHash()
			r.NoError(err)
			r.Equal(want, got, "the hash of recovered DB should be the same as of the original one")
		})
	}
}
