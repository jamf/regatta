// Copyright JAMF Software, LLC

package fsm

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	rp "github.com/jamf/regatta/pebble"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/stretchr/testify/require"
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
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT.
	_, err = handlePut(c, &regattapb.RequestOp_Put{
		Key:   []byte("key_1"),
		Value: []byte("value_1"),
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// Make the DELETE.
	res, err := handleDelete(c, &regattapb.RequestOp_DeleteRange{
		Key:    []byte("key_1"),
		PrevKv: true,
	})
	r.NoError(err)
	r.Equal(&regattapb.ResponseOp_DeleteRange{Deleted: 1, PrevKvs: []*regattapb.KeyValue{{Key: []byte("key_1"), Value: []byte("value_1")}}}, res)
	r.NoError(c.Commit())

	// Assert that there are no more user keys left.
	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)
	iter.First()
	r.False(iter.Valid())
	r.NoError(iter.Close())

	// Assert deleting non-existent key returns count 0.
	c.batch = db.NewBatch()
	res, err = handleDelete(c, &regattapb.RequestOp_DeleteRange{
		Key:   []byte("key_1"),
		Count: true,
	})
	r.NoError(err)
	r.Equal(&regattapb.ResponseOp_DeleteRange{Deleted: 0, PrevKvs: nil}, res)
	r.NoError(c.Commit())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
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
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	_, err = handlePutBatch(c, []*regattapb.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// Make the DELETE_BATCH.
	_, err = handleDeleteBatch(c, []*regattapb.RequestOp_DeleteRange{
		{Key: []byte("key_1")},
		{Key: []byte("key_2")},
		{Key: []byte("key_3")},
		{Key: []byte("key_4")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)

	// Skip the local index first and assert that there are no more keys in state machine.
	iter.First()
	r.False(iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
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
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	_, err = handlePutBatch(c, []*regattapb.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// Make the DELETE RANGE - delete first two user keys.
	_, err = handleDelete(c, &regattapb.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: []byte("key_3")})
	r.NoError(err)
	r.NoError(c.Commit())

	// Assert that there left expected user keys.
	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)
	iter.First()
	k := &key.Key{}
	decodeKey(t, iter, k)
	r.Equal([]byte("key_3"), k.Key)
	iter.Next()
	decodeKey(t, iter, k)
	r.Equal([]byte("key_4"), k.Key)

	// Skip the local index first and assert that there are no more keys in state machine.
	iter.Next()
	r.False(iter.Valid())
	r.NoError(iter.Close())

	c.batch = db.NewBatch()

	// Make the DELETE RANGE - delete the rest of the user keys.
	_, err = handleDelete(c, &regattapb.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: wildcard})
	r.NoError(err)
	r.NoError(c.Commit())

	// Skip the local index first and assert that there are no more keys in state machine.
	iter, err = db.NewIter(allUserKeysOpts())
	r.NoError(err)
	iter.First()
	r.False(iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
}
