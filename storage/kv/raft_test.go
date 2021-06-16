package kv

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

const snapshotFileName = "snapshot-000000000000000B"

func TestLFSM_RecoverFromSnapshot(t *testing.T) {
	r := require.New(t)
	snapshotPath := path.Join(t.TempDir(), snapshotFileName)
	stateMachine := NewLFSM()(1, 1).(*LFSM)
	stateMachine.data = map[string]Pair{
		"/foo": {
			Key:   "/foo",
			Value: "bar",
			Ver:   1,
		},
		"/foo/user": {
			Key:   "/foo/user",
			Value: "user",
			Ver:   2,
		},
	}

	t.Log("store snapshot")
	ctx, err := stateMachine.PrepareSnapshot()
	r.NoError(err)
	file, err := os.Create(snapshotPath)
	r.NoError(err)
	r.NoError(stateMachine.SaveSnapshot(ctx, file, nil, nil))
	r.NoError(file.Close())

	t.Log("load from snapshot")
	file, err = os.Open(snapshotPath)
	r.NoError(err)
	stateMachine2 := NewLFSM()(1, 1).(*LFSM)
	r.NoError(stateMachine2.RecoverFromSnapshot(file, nil, nil))

	r.Equal(stateMachine.data, stateMachine2.data)
}
