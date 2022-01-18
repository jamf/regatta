package fsm

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/wandera/regatta/proto"
)

func txnCompare(reader pebble.Reader, compare []*proto.Compare) (bool, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	result := true
	for _, cmp := range compare {
		if cmp.RangeEnd != nil {
			opts, err := iterOptionsForBounds(cmp.Key, cmp.RangeEnd)
			if err != nil {
				return false, err
			}
			iter := reader.NewIter(opts)
			for iter.First(); iter.Valid(); iter.Next() {
				result = result && txnCompareSingle(cmp, iter.Value())
			}
		} else {
			if err := encodeUserKey(keyBuf, cmp.Key); err != nil {
				return false, err
			}
			value, closer, err := reader.Get(keyBuf.Bytes())
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return false, err
			}

			result = result && txnCompareSingle(cmp, value)

			if closer != nil {
				if err := closer.Close(); err != nil {
					return false, err
				}
			}
			keyBuf.Reset()
		}
	}
	return result, nil
}

func txnCompareSingle(cmp *proto.Compare, value []byte) bool {
	keyExists := value != nil
	cmpValue := true
	if cmp.Target == proto.Compare_VALUE && cmp.GetValue() != nil {
		switch cmp.Result {
		case proto.Compare_EQUAL:
			cmpValue = bytes.Equal(value, cmp.GetValue())
		case proto.Compare_NOT_EQUAL:
			cmpValue = !bytes.Equal(value, cmp.GetValue())
		case proto.Compare_GREATER:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == -1
		case proto.Compare_LESS:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == +1
		}
	}

	return keyExists && cmpValue
}
