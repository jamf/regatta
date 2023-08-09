// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table/key"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const maxRangeSize uint64 = (4 * 1024 * 1024) - 1024 // 4MiB - 1KiB sentinel.

func commandSnapshot(reader pebble.Reader, tableName string, w io.Writer, stopc <-chan struct{}) (uint64, error) {
	iter := reader.NewIter(nil)
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
	opts, err := iterOptionsForBounds(req.Key, req.RangeEnd)
	if err != nil {
		return nil, err
	}
	iter := reader.NewIter(opts)
	defer func() {
		_ = iter.Close()
	}()
	fill, sf := iterFuncsFromReq(req)
	return iterate(iter, int(req.Limit), fill, sf)
}

func iterFuncsFromReq(req *regattapb.RequestOp_Range) (fillEntriesFunc, sizeEntriesFunc) {
	switch {
	case req.KeysOnly:
		return addKeyOnly, sizeKeyOnly
	case req.CountOnly:
		return addCountOnly, sizeCountOnly
	default:
		return addKVPair, sizeKVPair
	}
}

func singleLookup(reader pebble.Reader, req *regattapb.RequestOp_Range) (*regattapb.ResponseOp_Range, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)

	err := encodeUserKey(keyBuf, req.Key)
	if err != nil {
		return nil, err
	}

	iter := reader.NewIter(nil)
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

// fillEntriesFunc fills proto.RangeResponse response.
type fillEntriesFunc func(key, value []byte, response *regattapb.ResponseOp_Range)

// sizeEntriesFunc estimates entry size.
type sizeEntriesFunc func(key, value []byte) uint64

// iterate until the provided pebble.Iterator is no longer valid or the limit is reached.
// Apply a function on the key/value pair in every iteration filling proto.RangeResponse.
func iterate(iter *pebble.Iterator, limit int, f fillEntriesFunc, s sizeEntriesFunc) (*regattapb.ResponseOp_Range, error) {
	response := &regattapb.ResponseOp_Range{}
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := key.DecodeBytes(iter.Key())
		if err != nil {
			return nil, err
		}

		if i == limit && limit != 0 || (uint64(response.SizeVT())+s(k.Key, iter.Value())) >= maxRangeSize {
			response.More = iter.Next()
			break
		}
		i++
		f(k.Key, iter.Value(), response)
	}
	return response, nil
}

// addKVPair adds a key/value pair from the provided iterator to the proto.RangeResponse.
func addKVPair(key, value []byte, response *regattapb.ResponseOp_Range) {
	kv := &regattapb.KeyValue{Key: make([]byte, len(key)), Value: make([]byte, len(value))}
	copy(kv.Key, key)
	copy(kv.Value, value)
	response.Kvs = append(response.Kvs, kv)
	response.Count++
}

// sizeKVPair takes the full pair size into consideration.
func sizeKVPair(key, value []byte) uint64 {
	return uint64(len(key) + len(value))
}

// addKeyOnly adds a key from the provided iterator to the proto.RangeResponse.
func addKeyOnly(key, _ []byte, response *regattapb.ResponseOp_Range) {
	kv := &regattapb.KeyValue{Key: make([]byte, len(key))}
	copy(kv.Key, key)
	response.Kvs = append(response.Kvs, kv)
	response.Count++
}

// sizeKeyOnly takes only the key into consideration.
func sizeKeyOnly(key, _ []byte) uint64 {
	return uint64(len(key))
}

// addCountOnly increments number of keys from the provided iterator to the proto.RangeResponse.
func addCountOnly(_, _ []byte, response *regattapb.ResponseOp_Range) {
	response.Count++
}

// sizeCountOnly for count the size remains constant.
func sizeCountOnly(_, _ []byte) uint64 {
	return uint64(0)
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
