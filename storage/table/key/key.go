package key

import (
	"errors"
	"io"
)

type Type byte

const (
	TypeUnknown Type = iota
	TypeUser
	TypeSystem
)

const (
	keyHeaderLen        = 4
	keyVersionHeaderPos = 0
)

var (
	ErrUnknownKeyVersion = errors.New("unknown key version")
	ErrMissingKeyHeader  = errors.New("missing key header")
)

type Key struct {
	version uint8
	KeyType Type
	Key     []byte
}

func (k *Key) reset() {
	k.version = 0
	k.KeyType = 0
	k.Key = k.Key[0:0]
}

type Decoder struct {
	r io.Reader
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{r: reader}
}

func (d Decoder) Decode(key *Key) error {
	key.reset()
	var header [keyHeaderLen]byte
	_, err := io.ReadFull(d.r, header[:])
	if err != nil {
		return ErrMissingKeyHeader
	}

	switch header[keyVersionHeaderPos] {
	case keyV1Version:
		k := keyV1{}
		err := k.Decode(d.r)
		if err != nil {
			return err
		}
		key.version = keyV1Version
		key.KeyType = k.keyType
		key.Key = k.key
		return nil
	}
	return ErrUnknownKeyVersion
}

type Encoder struct {
	w io.Writer
}

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{w: writer}
}

func (e Encoder) Encode(key *Key) (int, error) {
	var header [keyHeaderLen]byte
	header[keyVersionHeaderPos] = key.version
	if _, err := e.w.Write(header[:]); err != nil {
		return 0, err
	}
	switch key.version {
	case keyV1Version:
		k := keyV1{}
		k.keyType = key.KeyType
		k.key = key.Key
		n, err := k.Encode(e.w)
		if err != nil {
			return keyHeaderLen, err
		}
		return keyHeaderLen + n, nil
	}
	return 0, ErrUnknownKeyVersion
}
