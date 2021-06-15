package kv

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

const snapshotFileName = "snapshot-000000000000000B"

func TestLFSM_RecoverFromSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	rs := newRaftStore(fs)
	fillData(rs, testData)
	r := require.New(t)

	snapshotPath := path.Join("tmp", "snapshot")
	r.NoError(fs.MkdirAll(snapshotPath, 0777))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	_, err := rs.NodeHost.SyncRequestSnapshot(ctx, rs.ClusterID, dragonboat.SnapshotOption{
		ExportPath: snapshotPath,
		Exported:   true,
	})
	r.NoError(err)

	_, err = fs.Stat(path.Join(snapshotPath, snapshotFileName, fmt.Sprintf("%s.gbsnap", snapshotFileName)))
	r.NoError(err)
	_, err = fs.Stat(path.Join(snapshotPath, snapshotFileName, "snapshot.metadata"))
	r.NoError(err)
}
