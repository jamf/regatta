// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tan

import (
	"bytes"
	"flag"
	"math"
	"os"
	"os/exec"
	"testing"

	"github.com/jamf/regatta/raft/config"
	pb "github.com/jamf/regatta/raft/raftpb"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

var spawnChild = flag.Bool("spawn-child", false, "spawned child")

func spawn(execName string) ([]byte, error) {
	return exec.Command(execName, "-spawn-child",
		"-test.v", "-test.run=TestFileLock$").CombinedOutput()
}

func TestFileLock(t *testing.T) {
	dbdir := "db-dir"
	child := *spawnChild
	msg := "failed to lock tan dir"
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{
			FS: vfs.Default,
		},
	}
	require.NoError(t, cfg.Prepare())
	if !child {
		ldb, err := CreateTan(cfg, nil, []string{dbdir}, nil)
		require.NoError(t, err)
		defer func() {
			ldb.Close()
			require.NoError(t, ldb.fs.RemoveAll(dbdir))
		}()
		out, err := spawn(os.Args[0])
		if err == nil {
			t.Fatalf("file lock didn't prevent the second tan to start, %s", out)
		}
		require.True(t, bytes.Contains(out, []byte(msg)))
	} else {
		ldb, err := CreateTan(cfg, nil, []string{dbdir}, nil)
		if err == nil {
			ldb.Close()
		} else {
			t.Fatalf(msg)
		}
	}
}

func TestListNodeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{
			FS: vfs.Default,
		},
	}
	require.NoError(t, cfg.Prepare())
	ldb, err := CreateTan(cfg, nil, []string{"db-dir"}, nil)
	require.NoError(t, err)
	defer func() {
		ldb.Close()
		require.NoError(t, ldb.fs.RemoveAll("db-dir"))
	}()
	rec := pb.Bootstrap{}
	require.NoError(t, ldb.SaveBootstrapInfo(1, 1, rec))
	require.NoError(t, ldb.SaveBootstrapInfo(2, 2, rec))
	require.NoError(t, ldb.SaveBootstrapInfo(3, 3, rec))
	nodes, err := ldb.ListNodeInfo()
	require.NoError(t, err)
	require.Equal(t, 3, len(nodes))
	for _, n := range nodes {
		require.True(t, n.ShardID == 1 && n.ReplicaID == 1 ||
			n.ShardID == 2 && n.ReplicaID == 2 ||
			n.ShardID == 3 && n.ReplicaID == 3)
	}
}

func TestLogDBCanBeCreated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{FS: vfs.NewMem()},
	}
	require.NoError(t, cfg.Prepare())
	dirs := []string{"db-dir"}
	ldb, err := CreateTan(cfg, nil, dirs, []string{})
	require.Equal(t, tanLogDBName, ldb.Name())
	require.NoError(t, err)
	require.NoError(t, ldb.Close())
}

func TestSaveSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{FS: vfs.NewMem()},
	}
	require.NoError(t, cfg.Prepare())
	dirs := []string{"db-dir"}
	ldb, err := CreateTan(cfg, nil, dirs, []string{})
	require.NoError(t, err)
	updates := []pb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Snapshot:  pb.Snapshot{Index: 100, Term: 10},
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Snapshot:  pb.Snapshot{Index: 200, Term: 10},
		},
	}
	require.NoError(t, ldb.SaveSnapshots(updates))
	ss1, err := ldb.GetSnapshot(1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(100), ss1.Index)
	ss2, err := ldb.GetSnapshot(2, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(200), ss2.Index)
	require.NoError(t, ldb.Close())
}

func TestSaveRaftState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{FS: vfs.NewMem()},
	}
	require.NoError(t, cfg.Prepare())
	dirs := []string{"db-dir"}
	ldb, err := CreateTan(cfg, nil, dirs, []string{})
	require.NoError(t, err)
	updates := []pb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Snapshot:  pb.Snapshot{Index: 100, Term: 10},
			State:     pb.State{Commit: 100, Term: 10},
			EntriesToSave: []pb.Entry{
				{Index: 99, Term: 10},
				{Index: 100, Term: 10},
			},
		},
		{
			ShardID:   17,
			ReplicaID: 1,
			Snapshot:  pb.Snapshot{Index: 200, Term: 10},
			State:     pb.State{Commit: 200, Term: 10},
			EntriesToSave: []pb.Entry{
				{Index: 198, Term: 10},
				{Index: 199, Term: 10},
				{Index: 200, Term: 10},
			},
		},
	}
	require.NoError(t, ldb.SaveRaftState(updates, 1))
	ss1, err := ldb.GetSnapshot(1, 1)
	require.NoError(t, err)
	require.Equal(t, updates[0].Snapshot, ss1)

	ss2, err := ldb.GetSnapshot(17, 1)
	require.NoError(t, err)
	require.Equal(t, updates[1].Snapshot, ss2)

	var entries []pb.Entry
	results, _, err := ldb.IterateEntries(entries, 0, 1, 1, 99, 101, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, 2, len(results))

	rs, err := ldb.ReadRaftState(1, 1, 98)
	require.NoError(t, err)
	require.Equal(t, updates[0].State, rs.State)
	require.NoError(t, ldb.Close())
}
