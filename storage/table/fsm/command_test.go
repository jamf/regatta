package fsm

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

func TestUpdateContext_Init(t *testing.T) {
	type args struct {
		entry sm.Entry
	}
	type want struct {
		index uint64
		cmd   *proto.Command
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "empty command",
			args: args{entry: sm.Entry{Cmd: nil}},
			want: want{index: 0, cmd: &proto.Command{}},
		},
		{
			name: "empty command with index",
			args: args{entry: sm.Entry{Index: 200}},
			want: want{index: 200, cmd: &proto.Command{}},
		},
		{
			name: "put command with index",
			args: args{entry: sm.Entry{Index: 200, Cmd: mustMarshallProto(&proto.Command{Type: proto.Command_PUT, Table: []byte("test"), Kv: &proto.KeyValue{Key: []byte("key")}})}},
			want: want{index: 200, cmd: &proto.Command{Type: proto.Command_PUT, Table: []byte("test"), Kv: &proto.KeyValue{Key: []byte("key"), Value: nil}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			uc := updateContext{
				cmd: &proto.Command{},
			}
			err := uc.Init(tt.args.entry)
			if tt.wantErr {
				r.Error(err)
			}
			r.NoError(err)
			r.Equal(tt.want.index, uc.index)
			r.Equal(tt.want.cmd.Table, uc.cmd.Table)
			r.Equal(tt.want.cmd.Type, uc.cmd.Type)
			r.Equal(tt.want.cmd.LeaderIndex, uc.cmd.LeaderIndex)
			r.Equal(tt.want.cmd.Txn, uc.cmd.Txn)
			r.Equal(tt.want.cmd.Kv, uc.cmd.Kv)
		})
	}
}

func TestUpdateContext_EnsureIndexed(t *testing.T) {
	r := require.New(t)
	db, err := rp.OpenDB("/", rp.WithFS(vfs.NewMem()))
	r.NoError(err)
	uc := updateContext{
		db:    db,
		batch: db.NewBatch(),
		cmd:   proto.CommandFromVTPool(),
	}
	tk := []byte("key")
	tv := []byte("value")
	r.NoError(uc.batch.Set(tk, tv, nil))

	r.Equal(false, uc.batch.Indexed())

	r.NoError(uc.EnsureIndexed())
	r.Equal(true, uc.batch.Indexed())
	_, _, err = uc.batch.Get(tk)
	r.NoError(err)

	r.NoError(uc.EnsureIndexed())
	r.Equal(true, uc.batch.Indexed())
}

func TestUpdateContext_Commit(t *testing.T) {
	r := require.New(t)
	db, err := rp.OpenDB("/", rp.WithFS(vfs.NewMem()))
	r.NoError(err)

	li := uint64(100)
	uc := updateContext{
		db:    db,
		batch: db.NewBatch(),
		cmd: &proto.Command{
			Type:        proto.Command_DUMMY,
			Table:       []byte("test"),
			LeaderIndex: &li,
		},
		index: 150,
	}
	r.NoError(uc.Commit())

	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(uc.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*uc.cmd.LeaderIndex, index)
}

// allKeysOpts returns *pebble.IterOptions for iterating over
// all the user keys.
func allUserKeysOpts() *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: mustEncodeKey(key.Key{
			KeyType: key.TypeUser,
			Key:     key.LatestMinKey,
		}),
		UpperBound: incrementRightmostByte(mustEncodeKey(key.Key{
			KeyType: key.TypeUser,
			Key:     key.LatestMaxKey,
		})),
	}
}

// decodeKey into *key.Key as pointed by the supplied *pebble.Iterator.
func decodeKey(t *testing.T, iter *pebble.Iterator, k *key.Key) {
	dec := key.NewDecoder(bytes.NewReader(iter.Key()))
	if err := dec.Decode(k); err != nil {
		t.Fatalf("could not decode key: %v", err)
	}
}
