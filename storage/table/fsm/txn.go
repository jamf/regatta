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
	for _, cmp := range compare {
		if cmp.RangeEnd != nil {
			opts, err := iterOptionsForBounds(cmp.Key, cmp.RangeEnd)
			if err != nil {
				return false, err
			}
			iter := reader.NewIter(opts)
			if !iter.First() {
				return false, nil
			}
			for iter.First(); iter.Valid(); iter.Next() {
				if !txnCompareSingle(cmp, iter.Value()) {
					return false, nil
				}
			}
		} else {
			res, err := func() (bool, error) {
				if err := encodeUserKey(keyBuf, cmp.Key); err != nil {
					return false, err
				}
				value, closer, err := reader.Get(keyBuf.Bytes())
				defer func() {
					if closer != nil {
						_ = closer.Close()
					}
				}()

				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						return false, nil
					}
					return false, err
				}

				if !txnCompareSingle(cmp, value) {
					return false, nil
				}

				keyBuf.Reset()
				return true, nil
			}()
			if err != nil {
				return false, err
			}
			if !res {
				return false, nil
			}
		}
	}
	return true, nil
}

func txnCompareSingle(cmp *proto.Compare, value []byte) bool {
	cmpValue := true
	if cmp.Target == proto.Compare_VALUE && cmp.TargetUnion != nil {
		switch cmp.Result {
		case proto.Compare_EQUAL:
			cmpValue = bytes.Equal(value, cmp.GetValue())
		case proto.Compare_NOT_EQUAL:
			cmpValue = !bytes.Equal(value, cmp.GetValue())
		case proto.Compare_GREATER:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == 1
		case proto.Compare_LESS:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == -1
		}
	}

	return cmpValue
}
