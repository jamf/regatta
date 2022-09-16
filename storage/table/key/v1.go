package key

import (
	"errors"
	"io"
)

var ErrMissingKeyType = errors.New("missing key type")

const (
	V1           uint8 = 1
	V1KeyLen           = 1024
	keyV1BodyLen       = V1KeyLen - keyHeaderLen
)

// V1Len computes length of a key.
func V1Len(userkeyLen int) int {
	return keyHeaderLen + 1 + userkeyLen
}

var V1MinKey = func() []byte {
	var minKey []byte
	for i := 0; i < keyV1BodyLen-1; i++ {
		minKey = append(minKey, 0)
	}
	return minKey
}

var V1MaxKey = func() []byte {
	var maxKey []byte
	for i := 0; i < keyV1BodyLen-1; i++ {
		maxKey = append(maxKey, 255)
	}
	return maxKey
}

type keyV1 struct {
	keyType Type
	key     []byte
}

func (k *keyV1) Encode(writer io.Writer) (int, error) {
	bytes := make([]byte, 1+len(k.key))
	bytes[0] = byte(k.keyType)
	copy(bytes[1:], k.key)
	return writer.Write(bytes[:])
}

func (k *keyV1) Decode(reader io.Reader) error {
	bytes, err := io.ReadAll(io.LimitReader(reader, int64(keyV1BodyLen)))
	if err != nil {
		return err
	}
	if len(bytes) < 1 {
		return ErrMissingKeyType
	}
	k.keyType = Type(bytes[0])
	k.key = bytes[1:]
	return nil
}
