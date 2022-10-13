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

func TestUpdateContext_Parse(t *testing.T) {
	type args struct {
		entry sm.Entry
	}
	type want struct {
		index uint64
		cmd   command
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
			want: want{index: 0, cmd: commandPut{}},
		},
		{
			name: "empty command with index",
			args: args{entry: sm.Entry{Index: 200}},
			want: want{index: 200, cmd: commandPut{}},
		},
		{
			name: "put command with index",
			args: args{entry: sm.Entry{Index: 200, Cmd: mustMarshallProto(&proto.Command{Type: proto.Command_PUT, Table: []byte("test"), Kv: &proto.KeyValue{Key: []byte("key")}})}},
			want: want{index: 200, cmd: commandPut{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			uc := updateContext{}
			cmd, err := parseCommand(&uc, tt.args.entry)
			if tt.wantErr {
				r.Error(err)
			}
			r.NoError(err)
			r.IsType(tt.want.cmd, cmd)
			r.Equal(tt.want.index, uc.index)
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

	uc := updateContext{
		db:    db,
		batch: db.NewBatch(),
		index: 150,
	}
	r.NoError(uc.Commit(&pebble.WriteOptions{}))

	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(uc.index, index)
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
