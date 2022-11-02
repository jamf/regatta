// Copyright JAMF Software, LLC

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
