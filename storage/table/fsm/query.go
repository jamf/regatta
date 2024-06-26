// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/jamf/regatta/util/iter"
)

const maxRangeSize uint64 = (4 * 1024 * 1024) - 1024 // 4MiB - 1KiB sentinel.

func commandSnapshot(reader pebble.Reader, tableName string, w io.Writer, stopc <-chan struct{}) (uint64, error) {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	idx, err := readLocalIndex(reader, sysLocalIndex)
	if err != nil {
		return 0, err
	}

	var buffer []byte
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return 0, sm.ErrSnapshotStopped
		default:
			k, err := key.DecodeBytes(iter.Key())
			if err != nil {
				return 0, err
			}
			if k.KeyType == key.TypeUser {
				buffer, err = writeCommand(tableName, k.Key, iter.Value(), buffer)
				if err != nil {
					return 0, err
				}
				if _, err := w.Write(buffer); err != nil {
					return 0, err
				}
			}
		}
	}
	return idx, nil
}

// writeCommand writes KV pair as PUT proto.Command into (optionally provided) buffer.
func writeCommand(tableName string, key []byte, val []byte, buffer []byte) ([]byte, error) {
	cmd := regattapb.CommandFromVTPool()
	defer cmd.ReturnToVTPool()
	cmd.Table = []byte(tableName)
	cmd.Type = regattapb.Command_PUT
	cmd.Kv = &regattapb.KeyValue{
		Key:   key,
		Value: val,
	}
	size := cmd.SizeVT()
	if cap(buffer) < size {
		buffer = make([]byte, size*2)
	}
	n, err := cmd.MarshalToSizedBufferVT(buffer[:size])
	if err != nil {
		return buffer, err
	}
	return buffer[:n], err
}

func readLocalIndex(db pebble.Reader, indexKey []byte) (idx uint64, err error) {
	indexVal, closer, err := db.Get(indexKey)
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return 0, err
		}
		return 0, nil
	}

	defer func() {
		err = closer.Close()
	}()

	return binary.LittleEndian.Uint64(indexVal), nil
}

func lookup(reader pebble.Reader, req *regattapb.RequestOp_Range) (*regattapb.ResponseOp_Range, error) {
	if req.RangeEnd != nil {
		return rangeLookup(reader, req)
	}
	return singleLookup(reader, req)
}

func rangeLookup(reader pebble.Reader, req *regattapb.RequestOp_Range) (*regattapb.ResponseOp_Range, error) {
	it, err := iterate(reader, req)
	if err != nil {
		return nil, err
	}
	return iter.First(it), nil
}

func singleLookup(reader pebble.Reader, req *regattapb.RequestOp_Range) (*regattapb.ResponseOp_Range, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)

	err := encodeUserKey(keyBuf, req.Key)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = iter.Close()
	}()
	found := iter.SeekPrefixGE(keyBuf.Bytes())
	if !found {
		return &regattapb.ResponseOp_Range{}, nil
	}

	kv := &regattapb.KeyValue{Key: req.Key}
	value := iter.Value()
	if !(req.KeysOnly || req.CountOnly) && len(value) > 0 {
		kv.Value = make([]byte, len(value))
		copy(kv.Value, value)
	}

	var kvs []*regattapb.KeyValue
	if !req.CountOnly {
		kvs = append(kvs, kv)
	}

	return &regattapb.ResponseOp_Range{
		Kvs:   kvs,
		Count: 1,
	}, nil
}

func iteratorLookup(reader pebble.Reader, req *regattapb.RequestOp_Range) (iter.Seq[*regattapb.ResponseOp_Range], error) {
	if req.RangeEnd != nil {
		return iterate(reader, req)
	}
	single, err := singleLookup(reader, req)
	if err != nil {
		return nil, err
	}
	return iter.From(single), nil
}

// IteratorRequest returns open pebble.Iterator it is an API consumer responsibility to close it.
type IteratorRequest struct {
	RangeOp *regattapb.RequestOp_Range
}

// SnapshotRequest to write Command snapshot into provided writer.
type SnapshotRequest struct {
	Writer  io.Writer
	Stopper <-chan struct{}
}

// SnapshotResponse returns local index to which the snapshot was created.
type SnapshotResponse struct {
	Index uint64
}

// LocalIndexRequest to read local index.
type LocalIndexRequest struct{}

// LeaderIndexRequest to read leader index.
type LeaderIndexRequest struct{}

// IndexResponse returns local index.
type IndexResponse struct {
	Index uint64
}

// PathRequest request data disk paths.
type PathRequest struct{}

// PathResponse returns SM data paths.
type PathResponse struct {
	Path string
}
