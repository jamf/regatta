package fsm

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

func Test_handlePut(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB("/", rp.WithFS(vfs.NewMem()))
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
		},
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT.
	req := &proto.RequestOp_Put{
		Key:   []byte("key_1"),
		Value: []byte("value_1"),
	}
	_, err = handlePut(c, req)
	r.NoError(err)
	r.NoError(c.Commit())

	// Make the PUT update.
	req = &proto.RequestOp_Put{
		Key:    []byte("key_1"),
		Value:  []byte("value_2"),
		PrevKv: true,
	}
	res, err := handlePut(c, req)
	r.NoError(err)
	r.Equal(&proto.ResponseOp_Put{PrevKv: &proto.KeyValue{Key: []byte("key_1"), Value: []byte("value_1")}}, res)
	r.NoError(c.Commit())

	iter := db.NewIter(allUserKeysOpts())
	iter.First()

	k := &key.Key{}
	decodeKey(t, iter, k)

	r.Equal(req.Key, k.Key)
	r.Equal(req.Value, iter.Value())

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

func Test_handlePutBatch(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB("/", rp.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		cmd: &proto.Command{
			LeaderIndex: &one,
		},
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	ops := []*proto.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	}
	_, err = handlePutBatch(c, ops)
	r.NoError(err)
	r.NoError(c.Commit())

	var (
		i int
		k = &key.Key{}
	)

	iter := db.NewIter(allUserKeysOpts())
	for iter.First(); iter.Valid(); iter.Next() {
		decodeKey(t, iter, k)

		r.Equal(ops[i].Key, k.Key)
		r.Equal(ops[i].Value, iter.Value())
		r.NoError(iter.Error())

		i++
	}
	r.Equal(len(ops), i)
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)

	index, err = readLocalIndex(db, sysLeaderIndex)
	r.NoError(err)
	r.Equal(*c.cmd.LeaderIndex, index)
}
