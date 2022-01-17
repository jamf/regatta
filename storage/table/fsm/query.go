package fsm

import (
	"bytes"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/table/key"
)

// Lookup locally looks up the data.
func (p *FSM) Lookup(l interface{}) (interface{}, error) {
	switch req := l.(type) {
	case *proto.RequestOp_RequestRange:
		db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
		if req.RequestRange.RangeEnd != nil {
			return rangeLookup(db, req)
		}

		buf := bufferPool.Get()
		defer bufferPool.Put(buf)
		return singleLookup(db, req, buf)
	case SnapshotRequest:
		idx, err := p.commandSnapshot(req.Writer, req.Stopper)
		if err != nil {
			return nil, err
		}
		return &SnapshotResponse{Index: idx}, nil
	case LocalIndexRequest:
		db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
		idx, err := readLocalIndex(db, sysLocalIndex)
		if err != nil {
			return nil, err
		}
		return &IndexResponse{Index: idx}, nil
	case LeaderIndexRequest:
		db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
		idx, err := readLocalIndex(db, sysLeaderIndex)
		if err != nil {
			return nil, err
		}
		return &IndexResponse{Index: idx}, nil
	case PathRequest:
		return &PathResponse{Path: p.dirname, WALPath: p.walDirname}, nil
	default:
		p.log.Warn("received unknown lookup request of type %t", req)
	}

	return nil, storage.ErrUnknownQueryType
}

func (p *FSM) commandSnapshot(w io.Writer, stopc <-chan struct{}) (uint64, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	snapshot := db.NewSnapshot()
	iter := snapshot.NewIter(nil)
	defer func() {
		if err := iter.Close(); err != nil {
			p.log.Error(err)
		}
		if err := snapshot.Close(); err != nil {
			p.log.Error(err)
		}
	}()

	idx, err := readLocalIndex(snapshot, sysLocalIndex)
	if err != nil {
		return 0, err
	}

	var k key.Key
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return 0, sm.ErrSnapshotStopped
		default:
			dec := key.NewDecoder(bytes.NewReader(iter.Key()))
			err := dec.Decode(&k)
			if err != nil {
				return 0, err
			}
			if k.KeyType == key.TypeUser {
				if err := writeCommand(w, &proto.Command{
					Table: []byte(p.tableName),
					Type:  proto.Command_PUT,
					Kv: &proto.KeyValue{
						Key:   k.Key,
						Value: iter.Value(),
					},
				}); err != nil {
					return 0, err
				}
			}
		}
	}
	return idx, nil
}

func rangeLookup(db pebble.Reader, req *proto.RequestOp_RequestRange) (*proto.ResponseOp_Range, error) {
	iter, fill, err := iterator(db, req)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = iter.Close()
	}()

	response := &proto.ResponseOp_Range{}
	if err = iterate(iter, int(req.RequestRange.Limit), fill, response); err != nil {
		return nil, err
	}

	return response, nil
}

func singleLookup(db pebble.Reader, req *proto.RequestOp_RequestRange, keyBuf *bytes.Buffer) (*proto.ResponseOp_Range, error) {
	err := encodeUserKey(keyBuf, req.RequestRange.Key)
	if err != nil {
		return nil, err
	}

	value, closer, err := db.Get(keyBuf.Bytes())
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = closer.Close()
	}()

	kv := &proto.KeyValue{Key: req.RequestRange.Key}

	if !(req.RequestRange.KeysOnly || req.RequestRange.CountOnly) {
		kv.Value = make([]byte, len(value))
		copy(kv.Value, value)
	}

	var kvs []*proto.KeyValue
	if !req.RequestRange.CountOnly {
		kvs = append(kvs, kv)
	}

	return &proto.ResponseOp_Range{
		Kvs:   kvs,
		Count: 1,
	}, nil
}

func writeCommand(w io.Writer, command *proto.Command) error {
	bts, err := command.MarshalVT()
	if err != nil {
		return err
	}
	_, err = w.Write(bts)
	if err != nil {
		return err
	}
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
	Path    string
	WALPath string
}
