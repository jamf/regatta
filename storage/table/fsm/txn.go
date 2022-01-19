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
			err := func() error {
				if err := encodeUserKey(keyBuf, cmp.Key); err != nil {
					return err
				}
				value, closer, err := reader.Get(keyBuf.Bytes())
				defer func() {
					if closer != nil {
						_ = closer.Close()
					}
				}()

				if err != nil && !errors.Is(err, pebble.ErrNotFound) {
					return err
				}

				result = !errors.Is(err, pebble.ErrNotFound) && txnCompareSingle(cmp, value)

				keyBuf.Reset()
				return nil
			}()
			if err != nil {
				return false, err
			}
		}
	}
	return result, nil
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
