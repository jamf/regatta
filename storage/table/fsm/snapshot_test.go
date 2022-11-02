// Copyright JAMF Software, LLC

package fsm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestSM_Snapshot(t *testing.T) {
	type args struct {
		producingSMFactory func() *FSM
		receivingSMFactory func() *FSM
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"small values",
			args{
				producingSMFactory: filledSM,
				receivingSMFactory: emptySM,
			},
		},
		{
			"large values",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
			},
		},
		{
			"index only",
			args{
				producingSMFactory: filledIndexOnlySM,
				receivingSMFactory: emptySM,
			},
		},
	}
	for _, tt := range tests {
		t.Log("applying snapshot to the empty DB should produce the same hash")
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.args.producingSMFactory()
			defer p.Close()

			want, err := p.GetHash()
			r.NoError(err)

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()
			defer ep.Close()

			snapf, err := os.Create(filepath.Join(t.TempDir(), "snapshot-file"))
			if err == nil {
				defer snapf.Close()
			}
			r.NoError(err)

			t.Log("save snapshot started")
			err = p.SaveSnapshot(snp, snapf, nil)
			r.NoError(err)
			_, err = snapf.Seek(0, 0)
			r.NoError(err)

			t.Log("recover from snapshot started")
			stopc := make(chan struct{})
			err = ep.RecoverFromSnapshot(snapf, stopc)
			r.NoError(err)

			t.Log("recovery finished")

			got, err := ep.GetHash()
			r.NoError(err)
			r.Equal(want, got, "the hash of recovered DB should be the same as of the original one")

			l, err := p.Lookup(LocalIndexRequest{})
			r.NoError(err)
			el, err := ep.Lookup(LocalIndexRequest{})
			r.NoError(err)

			originIndex := l.(*IndexResponse).Index

			r.Equal(originIndex, el.(*IndexResponse).Index, "the applied indexes should be the same")

			r.NoError(ep.Close())
			idx, err := ep.Open(nil)
			r.NoError(err)
			r.Equal(originIndex, idx, "indexes after reopen should be the same")
		})
	}
}

func TestSM_Snapshot_Stopped(t *testing.T) {
	type args struct {
		producingSMFactory func() *FSM
		receivingSMFactory func() *FSM
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"Pebble(large) -> Pebble",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
			},
		},
	}
	for _, tt := range tests {
		t.Log("Applying snapshot to the empty DB should be stopped")
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.args.producingSMFactory()
			defer p.Close()

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()

			snapf, err := os.Create(filepath.Join(t.TempDir(), "snapshot-file"))
			r.NoError(err)
			t.Log("Save snapshot started")
			err = p.SaveSnapshot(snp, snapf, nil)
			r.NoError(err)
			_, err = snapf.Seek(0, 0)
			r.NoError(err)

			stopc := make(chan struct{})
			go func() {
				defer func() {
					_ = snapf.Close()
					_ = ep.Close()
					_ = snapf.Close()
				}()
				t.Log("Recover from snapshot routine started")
				err := ep.RecoverFromSnapshot(snapf, stopc)
				r.Error(err)
				r.Equal(sm.ErrSnapshotStopped, err)
			}()

			time.Sleep(10 * time.Millisecond)
			close(stopc)

			t.Log("Recovery stopped")
		})
	}
}
