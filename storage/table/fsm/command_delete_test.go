package fsm

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

func Test_handleDelete(t *testing.T) {
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
			LeaderIndex: &one,
		},
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT.
	_, err = handlePut(c, &proto.RequestOp_Put{
		Key:   []byte("key_1"),
		Value: []byte("value_1"),
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// Make the DELETE.
	res, err := handleDelete(c, &proto.RequestOp_DeleteRange{
		Key:    []byte("key_1"),
		PrevKv: true,
	})
	r.NoError(err)
	r.Equal(&proto.ResponseOp_DeleteRange{PrevKvs: []*proto.KeyValue{{Key: []byte("key_1"), Value: []byte("value_1")}}}, res)
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

func Test_handleDeleteBatch(t *testing.T) {
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
	_, err = handlePutBatch(c, []*proto.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()
	for i := range c.cmd.Batch {
		c.cmd.Batch[i].Value = nil
	}

	// Make the DELETE_BATCH.
	_, err = handleDeleteBatch(c, []*proto.RequestOp_DeleteRange{
		{Key: []byte("key_1")},
		{Key: []byte("key_2")},
		{Key: []byte("key_3")},
		{Key: []byte("key_4")},
	})
	r.NoError(err)
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

func Test_handleDeleteRange(t *testing.T) {
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
	_, err = handlePutBatch(c, []*proto.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// Make the DELETE RANGE - delete first two user keys.
	_, err = handleDelete(c, &proto.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: []byte("key_3")})
	r.NoError(err)
	r.NoError(c.Commit())

	// Assert that there left expected user keys.
	iter := db.NewIter(allUserKeysOpts())
	iter.First()
	k := &key.Key{}
	decodeKey(t, iter, k)
	r.Equal([]byte("key_3"), k.Key)
	iter.Next()
	decodeKey(t, iter, k)
	r.Equal([]byte("key_4"), k.Key)

	// Skip the local index first and assert that there are no more keys in state machine.
	iter.Next()
	r.Equal(false, iter.Valid())
	r.NoError(iter.Close())

	c.batch = db.NewBatch()

	// Make the DELETE RANGE - delete the rest of the user keys.
	_, err = handleDelete(c, &proto.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: wildcard})
	r.NoError(err)
	r.NoError(c.Commit())

	// Skip the local index first and assert that there are no more keys in state machine.
	iter = db.NewIter(allUserKeysOpts())
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
	r.NoError(iter.Close())
}
