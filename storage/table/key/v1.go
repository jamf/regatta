package key

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrMissingKeyType = errors.New("missing key type")

const (
	V1       uint8  = 1
	keyV1Len uint32 = 1024 - keyHeaderLen
)

type keyV1 struct {
	keyType Type
	key     []byte
}

func (k keyV1) Encode(writer io.Writer) (int, error) {
	if err := binary.Write(writer, binary.BigEndian, k.keyType); err != nil {
		return 0, err
	}
	if err := binary.Write(writer, binary.BigEndian, k.key); err != nil {
		return 1, err
	}
	return 1 + len(k.key), nil
}

func (k *keyV1) Decode(reader io.Reader) error {
	bytes, err := io.ReadAll(io.LimitReader(reader, int64(keyV1Len)))
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
