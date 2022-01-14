package fsm

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

var (
	one = uint64(1)
	two = uint64(2)
)

func TestSM_Update(t *testing.T) {
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
	}
	type args struct {
		updates []sm.Entry
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		want            []sm.Entry
		wantErr         bool
		wantLeaderIndex uint64
	}{
		{
			name: "Pebble - Successful update of a single item",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful bump of a leader index",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DUMMY,
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful update of a batch",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &two,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
							Kv: &proto.KeyValue{
								Key: []byte("test"),
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 2,
		},
		{
			name: "Pebble - Successful update of single batched PUT",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT_BATCH,
							Batch: []*proto.KeyValue{
								{
									Key:   []byte("test"),
									Value: []byte("test"),
								},
								{
									Key:   []byte("test1"),
									Value: []byte("test"),
								},
								{
									Key:   []byte("test2"),
									Value: []byte("test"),
								},
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful update of single batched DELETE",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE_BATCH,
							Batch: []*proto.KeyValue{
								{
									Key: []byte("test"),
								},
								{
									Key: []byte("test1"),
								},
								{
									Key: []byte("test2"),
								},
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful DELETE range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
							Kv:          &proto.KeyValue{Key: []byte("test")},
							RangeEnd:    incrementRightmostByte([]byte("test")),
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful DELETE all range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
							Kv:          &proto.KeyValue{Key: []byte("test")},
							RangeEnd:    []byte{0},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Update(tt.args.updates)
			if tt.wantErr {
				r.Error(err)
				return
			}

			maxIndex := uint64(0)
			for i, entry := range got {
				r.Equal(tt.want[i].Index, entry.Index)
				r.Equal(tt.want[i].Result, entry.Result)
				maxIndex = entry.Index
			}

			// Test whether the index is stored.
			res, err := p.Lookup(LocalIndexRequest{})
			r.NoError(err)
			indexRes, ok := res.(*IndexResponse)
			if !ok {
				r.Fail("could not cast response to *IndexResponse")
			}
			r.Equal(indexRes.Index, maxIndex)

			// Test whether the leaderIndex has changed.
			res, err = p.Lookup(LeaderIndexRequest{})
			r.NoError(err)
			indexRes, ok = res.(*IndexResponse)
			if !ok {
				r.Fail("could not cast response to *IndexResponse")
			}
			r.Equal(tt.wantLeaderIndex, indexRes.Index)
		})
	}
}

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
				cmd:    &proto.Command{},
				keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
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
			r.Equal(0, uc.keyBuf.Len())
		})
	}
}

func TestUpdateContext_EnsureIndexed(t *testing.T) {
	r := require.New(t)
	db, err := rp.OpenDB(vfs.NewMem(), "/", "/", nil)
	r.NoError(err)
	uc := updateContext{
		db:     db,
		batch:  db.NewBatch(),
		cmd:    proto.CommandFromVTPool(),
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
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
	db, err := rp.OpenDB(vfs.NewMem(), "/", "/", nil)
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

func TestHandlePut(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB(vfs.NewMem(), "", "", pebble.NewCache(0))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		cmd: &proto.Command{
			Table:       []byte("test"),
			Type:        proto.Command_PUT,
			LeaderIndex: &one,
			Kv: &proto.KeyValue{
				Key:   []byte("key_1"),
				Value: []byte("value_1"),
			},
		},
		index:  1,
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
	}
	defer func() { _ = c.Close() }()

	// Make the PUT.
	res, err := handlePut(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	iter := db.NewIter(allUserKeysOpts())
	iter.First()

	k := &key.Key{}
	decodeKey(t, iter, k)

	r.Equal(c.cmd.Kv.Key, k.Key)
	r.Equal(c.cmd.Kv.Value, iter.Value())

	// Assert that there are no more user keys.
	iter.Next()
	r.Equal(false, iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*c.cmd.LeaderIndex, index)
}

func TestHandleDelete(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB(vfs.NewMem(), "", "", pebble.NewCache(0))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		cmd: &proto.Command{
			Table:       []byte("test"),
			Type:        proto.Command_PUT,
			LeaderIndex: &one,
			Kv: &proto.KeyValue{
				Key:   []byte("key_1"),
				Value: []byte("value_1"),
			},
		},
		index:  1,
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
	}
	defer func() { _ = c.Close() }()

	// Make the PUT.
	res, err := handlePut(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()
	c.cmd.Type = proto.Command_DELETE
	c.cmd.Kv.Value = nil
	c.keyBuf = bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen))

	// Make the DELETE.
	res, err = handleDelete(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	// Assert that there are no more user keys left.
	iter := db.NewIter(allUserKeysOpts())
	iter.First()
	r.Equal(false, iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*c.cmd.LeaderIndex, index)
}

func TestHandlePutBatch(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB(vfs.NewMem(), "", "", pebble.NewCache(0))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		cmd: &proto.Command{
			Table:       []byte("test"),
			Type:        proto.Command_PUT_BATCH,
			LeaderIndex: &one,
			Batch: []*proto.KeyValue{
				{
					Key:   []byte("key_1"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_2"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_3"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_4"),
					Value: []byte("value"),
				},
			},
		},
		index:  1,
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	res, err := handlePutBatch(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	var (
		i int
		k = &key.Key{}
	)

	iter := db.NewIter(allUserKeysOpts())
	for iter.First(); iter.Valid(); iter.Next() {
		decodeKey(t, iter, k)

		r.Equal(c.cmd.Batch[i].Key, k.Key)
		r.Equal(c.cmd.Batch[i].Value, iter.Value())
		r.NoError(iter.Error())

		i++
	}
	r.Equal(len(c.cmd.Batch), i)
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*c.cmd.LeaderIndex, index)
}

func TestHandleDeleteBatch(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB(vfs.NewMem(), "", "", pebble.NewCache(0))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		cmd: &proto.Command{
			Table:       []byte("test"),
			Type:        proto.Command_PUT_BATCH,
			LeaderIndex: &one,
			Batch: []*proto.KeyValue{
				{
					Key:   []byte("key_1"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_2"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_3"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_4"),
					Value: []byte("value"),
				},
			},
		},
		index:  1,
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	res, err := handlePutBatch(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()
	c.cmd.Type = proto.Command_DELETE_BATCH
	c.keyBuf = bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen))
	for i := range c.cmd.Batch {
		c.cmd.Batch[i].Value = nil
	}

	// Make the DELETE_BATCH.
	res, err = handleDeleteBatch(c)
	r.NoError(err)
	r.Equal(sm.Result{Value: ResultSuccess}, res)
	r.NoError(c.Commit())

	iter := db.NewIter(allUserKeysOpts())

	// Skip the local index first and assert that there are no more keys in state machine.
	iter.First()
	r.Equal(false, iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*c.cmd.LeaderIndex, index)
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
