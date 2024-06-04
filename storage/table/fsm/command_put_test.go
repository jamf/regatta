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

func Test_handlePut(t *testing.T) {
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
	req := &regattapb.RequestOp_Put{
		Key:    []byte("key_1"),
		Value:  []byte("value_1"),
		PrevKv: true,
	}
	res, err := handlePut(c, req)
	r.NoError(err)
	r.Equal(&regattapb.ResponseOp_Put{}, res)
	r.NoError(c.Commit())

	// Make the PUT update.
	req = &regattapb.RequestOp_Put{
		Key:    []byte("key_1"),
		Value:  []byte("value_2"),
		PrevKv: true,
	}

	c.batch = db.NewBatch()
	res, err = handlePut(c, req)
	r.NoError(err)
	r.Equal(&regattapb.ResponseOp_Put{PrevKv: &regattapb.KeyValue{Key: []byte("key_1"), Value: []byte("value_1")}}, res)
	r.NoError(c.Commit())

	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)
	iter.First()

	k := &key.Key{}
	decodeKey(t, iter, k)

	r.Equal(req.Key, k.Key)
	r.Equal(req.Value, iter.Value())

	// Assert that there are no more user keys.
	iter.Next()
	r.False(iter.Valid())
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
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
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	ops := []*regattapb.RequestOp_Put{
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

	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)
	for iter.First(); iter.Valid(); iter.Next() {
		decodeKey(t, iter, k)

		r.Equal(ops[i].Key, k.Key)
		r.Equal(ops[i].Value, iter.Value())
		r.NoError(iter.Error())

		i++
	}
	r.Len(ops, i)
	r.NoError(iter.Close())

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
}
