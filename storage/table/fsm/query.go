// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/errors"
	"github.com/wandera/regatta/storage/table/key"
)

// Lookup locally looks up the data.
func (p *FSM) Lookup(l interface{}) (interface{}, error) {
	switch req := l.(type) {
	case *proto.TxnRequest:
		snapshot := p.pebble.Load().NewSnapshot()
		defer snapshot.Close()

		ok, err := txnCompare(snapshot, req.Compare)
		if err != nil {
			return nil, err
		}

		var ops []*proto.RequestOp_Range
		if ok {
			for _, op := range req.Success {
				ops = append(ops, op.GetRequestRange())
			}
		} else {
			for _, op := range req.Failure {
				ops = append(ops, op.GetRequestRange())
			}
		}

		resp := &proto.TxnResponse{Succeeded: ok}
		for _, op := range ops {
			rr, err := lookup(snapshot, op)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, wrapResponseOp(rr))
		}
		return resp, nil
	case *proto.RequestOp_Range:
		db := p.pebble.Load()
		return lookup(db, req)
	case SnapshotRequest:
		snapshot := p.pebble.Load().NewSnapshot()
		defer snapshot.Close()

		idx, err := commandSnapshot(snapshot, p.tableName, req.Writer, req.Stopper)
		if err != nil {
			return nil, err
		}
		return &SnapshotResponse{Index: idx}, nil
	case LocalIndexRequest:
		idx, err := readLocalIndex(p.pebble.Load(), sysLocalIndex)
		if err != nil {
			return nil, err
		}
		return &IndexResponse{Index: idx}, nil
	case LeaderIndexRequest:
		idx, err := readLocalIndex(p.pebble.Load(), sysLeaderIndex)
		if err != nil {
			return nil, err
		}
		return &IndexResponse{Index: idx}, nil
	case PathRequest:
		return &PathResponse{Path: p.dirname}, nil
	default:
		p.log.Warnf("received unknown lookup request of type %T", req)
	}

	return nil, errors.ErrUnknownQueryType
}

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
	cmd := proto.CommandFromVTPool()
	defer cmd.ReturnToVTPool()
	cmd.Table = []byte(tableName)
	cmd.Type = proto.Command_PUT
	cmd.Kv = &proto.KeyValue{
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
		if err != pebble.ErrNotFound {
			return 0, err
		}
		return 0, nil
	}

	defer func() {
		err = closer.Close()
	}()

	return binary.LittleEndian.Uint64(indexVal), nil
}

func lookup(reader pebble.Reader, req *proto.RequestOp_Range) (*proto.ResponseOp_Range, error) {
	if req.RangeEnd != nil {
		return rangeLookup(reader, req)
	}
	return singleLookup(reader, req)
}

func rangeLookup(reader pebble.Reader, req *proto.RequestOp_Range) (*proto.ResponseOp_Range, error) {
	opts, err := iterOptionsForBounds(req.Key, req.RangeEnd)
	if err != nil {
		return nil, err
	}
	iter := reader.NewIter(opts)
	defer func() {
		_ = iter.Close()
	}()

	fill := addKVPair
	if req.KeysOnly {
		fill = addKeyOnly
	} else if req.CountOnly {
		fill = addCountOnly
	}

	response := &proto.ResponseOp_Range{}
	if err = iterate(iter, int(req.Limit), fill, response); err != nil {
		return nil, err
	}

	return response, nil
}

func singleLookup(reader pebble.Reader, req *proto.RequestOp_Range) (*proto.ResponseOp_Range, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)

	err := encodeUserKey(keyBuf, req.Key)
	if err != nil {
		return nil, err
	}

	value, closer, err := reader.Get(keyBuf.Bytes())
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = closer.Close()
	}()

	kv := &proto.KeyValue{Key: req.Key}

	if !(req.KeysOnly || req.CountOnly) && len(value) > 0 {
		kv.Value = make([]byte, len(value))
		copy(kv.Value, value)
	}

	var kvs []*proto.KeyValue
	if !req.CountOnly {
		kvs = append(kvs, kv)
	}

	return &proto.ResponseOp_Range{
		Kvs:   kvs,
		Count: 1,
	}, nil
}

// fillEntriesFunc fills proto.RangeResponse response.
type fillEntriesFunc func(key, value []byte, response *proto.ResponseOp_Range) error

// iterate until the provided pebble.Iterator is no longer valid or the limit is reached.
// Apply a function on the key/value pair in every iteration filling proto.RangeResponse.
func iterate(iter *pebble.Iterator, limit int, f fillEntriesFunc, response *proto.ResponseOp_Range) error {
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := key.DecodeBytes(iter.Key())
		if err != nil {
			return err
		}

		if i == limit && limit != 0 {
			response.More = iter.Next()
			break
		}
		i++

		if err := f(k.Key, iter.Value(), response); err != nil {
			return err
		}
	}
	return nil
}

// addKVPair adds a key/value pair from the provided iterator to the proto.RangeResponse.
func addKVPair(key, value []byte, response *proto.ResponseOp_Range) error {
	kv := &proto.KeyValue{Key: make([]byte, len(key)), Value: make([]byte, len(value))}
	copy(kv.Key, key)
	copy(kv.Value, value)
	response.Kvs = append(response.Kvs, kv)
	response.Count++
	return nil
}

// addKeyOnly adds a key from the provided iterator to the proto.RangeResponse.
func addKeyOnly(key, _ []byte, response *proto.ResponseOp_Range) error {
	kv := &proto.KeyValue{Key: make([]byte, len(key))}
	copy(kv.Key, key)
	response.Kvs = append(response.Kvs, kv)
	response.Count++
	return nil
}

// addCountOnly increments number of keys from the provided iterator to the proto.RangeResponse.
func addCountOnly(_, _ []byte, response *proto.ResponseOp_Range) error {
	response.Count++
	return nil
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
