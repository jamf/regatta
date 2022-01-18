package fsm

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

func iterOptionsForBounds(low, high []byte) (*pebble.IterOptions, error) {
	lowBuf := bufferPool.Get()
	defer bufferPool.Put(lowBuf)
	if err := encodeUserKey(lowBuf, low); err != nil {
		return nil, err
	}

	highBuf := bufferPool.Get()
	defer bufferPool.Put(highBuf)

	if err := encodeUserKey(highBuf, high); err != nil {
		return nil, err
	}

	iterOptions := &pebble.IterOptions{
		LowerBound: make([]byte, lowBuf.Len()),
		UpperBound: make([]byte, highBuf.Len()),
	}
	copy(iterOptions.LowerBound, lowBuf.Bytes())

	if bytes.Equal(high, wildcard) {
		// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum user key.
		iterOptions.UpperBound = incrementRightmostByte(maxUserKey)
	} else {
		copy(iterOptions.UpperBound, highBuf.Bytes())
	}

	return iterOptions, nil
}
