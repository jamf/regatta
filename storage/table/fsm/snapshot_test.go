// Copyright JAMF Software, LLC

package fsm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/stretchr/testify/require"
)

func TestFSM_Snapshot(t *testing.T) {
	type args struct {
		producingSMFactory func() *FSM
		receivingSMFactory func() *FSM
		snapshotType       SnapshotRecoveryType
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"recover using snapshot (small DB)",
			args{
				producingSMFactory: filledSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeSnapshot,
			},
		},
		{
			"recover using snapshot (large DB)",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeSnapshot,
			},
		},
		{
			"recover using snapshot (empty DB)",
			args{
				producingSMFactory: filledIndexOnlySM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeSnapshot,
			},
		},
		{
			"recover using checkpoint (small DB)",
			args{
				producingSMFactory: filledSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeCheckpoint,
			},
		},
		{
			"recover using checkpoint (large DB)",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeCheckpoint,
			},
		},
		{
			"recover using checkpoint (empty DB)",
			args{
				producingSMFactory: filledIndexOnlySM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeCheckpoint,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			p := tt.args.producingSMFactory()
			p.recoveryType = tt.args.snapshotType
			defer p.Close()

			want, err := p.GetHash()
			r.NoError(err)

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()
			ep.recoveryType = tt.args.snapshotType
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

func TestFSM_SnapshotStopped(t *testing.T) {
	type args struct {
		producingSMFactory func() *FSM
		receivingSMFactory func() *FSM
		snapshotType       SnapshotRecoveryType
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"stop recovery using snapshot",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeSnapshot,
			},
		},
		{
			"stop recovery using checkpoint",
			args{
				producingSMFactory: filledLargeValuesSM,
				receivingSMFactory: emptySM,
				snapshotType:       RecoveryTypeCheckpoint,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.args.producingSMFactory()
			p.recoveryType = tt.args.snapshotType
			defer p.Close()

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()
			ep.recoveryType = tt.args.snapshotType

			snapf, err := os.Create(filepath.Join(t.TempDir(), "snapshot-file"))
			r.NoError(err)
			t.Log("Save snapshot started")
			err = p.SaveSnapshot(snp, snapf, nil)
			r.NoError(err)
			_, err = snapf.Seek(0, 0)
			r.NoError(err)

			resultCh := make(chan error)
			stopc := make(chan struct{})
			go func() {
				defer func() {
					_ = snapf.Close()
					_ = ep.Close()
					_ = snapf.Close()
				}()
				t.Log("Recover from snapshot routine started")
				resultCh <- ep.RecoverFromSnapshot(snapf, stopc)
			}()

			time.Sleep(10 * time.Millisecond)
			close(stopc)

			err = <-resultCh
			r.ErrorIs(err, sm.ErrSnapshotStopped)

			t.Log("Recovery stopped")
		})
	}
}
