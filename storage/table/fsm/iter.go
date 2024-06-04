// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/jamf/regatta/util/iter"
)

// iterate until the provided pebble.Iterator is no longer valid or the limit is reached.
// Apply a function on the key/value pair in every iteration filling proto.RangeResponse.
func iterate(reader pebble.Reader, req *regattapb.RequestOp_Range) (iter.Seq[*regattapb.ResponseOp_Range], error) {
	opts, err := iterOptionsForBounds(req.Key, req.RangeEnd)
	if err != nil {
		return nil, err
	}
	fill, sf := iterFuncsFromReq(req)
	limit := int(req.Limit)

	return func(yield func(*regattapb.ResponseOp_Range) bool) {
		piter, err := reader.NewIter(opts)
		if err != nil {
			return
		}
		defer func() {
			_ = piter.Close()
		}()
		response := &regattapb.ResponseOp_Range{}
		// If no results found yield and end immediately.
		if !piter.First() {
			yield(response)
			return
		}
		i := 0
		for {
			k, err := key.DecodeBytes(piter.Key())
			if err != nil {
				panic(err)
			}
			if i == limit && limit != 0 {
				response.More = piter.Next()
				yield(response)
				return
			}
			if (uint64(response.SizeVT()) + sf(k.Key, piter.ValueAndErr)) >= maxRangeSize {
				response.More = true
				if !yield(response) {
					return
				}
				response = &regattapb.ResponseOp_Range{}
			}
			i++
			fill(k.Key, piter.ValueAndErr, response)
			if !piter.Next() {
				yield(response)
				return
			}
		}
	}, nil
}

func iterOptionsForBounds(low, high []byte) (*pebble.IterOptions, error) {
	lowBuf := bufferPool.Get()
	defer bufferPool.Put(lowBuf)
	if err := encodeUserKey(lowBuf, low); err != nil {
		return nil, err
	}
	iterOptions := &pebble.IterOptions{
		LowerBound: make([]byte, lowBuf.Len()),
	}
	copy(iterOptions.LowerBound, lowBuf.Bytes())

	if bytes.Equal(high, wildcard) {
		// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum user key.
		iterOptions.UpperBound = make([]byte, len(maxUserKey))
		copy(iterOptions.UpperBound, maxUserKey)
		iterOptions.UpperBound = incrementRightmostByte(iterOptions.UpperBound)
	} else {
		highBuf := bufferPool.Get()
		defer bufferPool.Put(highBuf)

		if err := encodeUserKey(highBuf, high); err != nil {
			return nil, err
		}
		iterOptions.UpperBound = make([]byte, highBuf.Len())
		copy(iterOptions.UpperBound, highBuf.Bytes())
	}

	return iterOptions, nil
}

type lazyValueOrErr func() ([]byte, error)

// fillEntriesFunc fills proto.RangeResponse response.
type fillEntriesFunc func(key []byte, value lazyValueOrErr, response *regattapb.ResponseOp_Range)

// sizeEntriesFunc estimates entry size.
type sizeEntriesFunc func(key []byte, value lazyValueOrErr) uint64

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

// addKVPair adds a key/value pair from the provided iterator to the proto.RangeResponse.
func addKVPair(key []byte, value lazyValueOrErr, response *regattapb.ResponseOp_Range) {
	val, _ := value()
	kv := &regattapb.KeyValue{Key: make([]byte, len(key)), Value: make([]byte, len(val))}
	copy(kv.Key, key)
	copy(kv.Value, val)
	response.Kvs = append(response.Kvs, kv)
	response.Count = int64(len(response.Kvs))
}

// sizeKVPair takes the full pair size into consideration.
func sizeKVPair(key []byte, value lazyValueOrErr) uint64 {
	val, _ := value()
	return uint64(len(key) + len(val))
}

// addKeyOnly adds a key from the provided iterator to the proto.RangeResponse.
func addKeyOnly(key []byte, _ lazyValueOrErr, response *regattapb.ResponseOp_Range) {
	kv := &regattapb.KeyValue{Key: make([]byte, len(key))}
	copy(kv.Key, key)
	response.Kvs = append(response.Kvs, kv)
	response.Count++
}

// sizeKeyOnly takes only the key into consideration.
func sizeKeyOnly(key []byte, _ lazyValueOrErr) uint64 {
	return uint64(len(key))
}

// addCountOnly increments number of keys from the provided iterator to the proto.RangeResponse.
func addCountOnly(_ []byte, _ lazyValueOrErr, response *regattapb.ResponseOp_Range) {
	response.Count++
}

// sizeCountOnly for count the size remains constant.
func sizeCountOnly(_ []byte, _ lazyValueOrErr) uint64 {
	return uint64(0)
}
