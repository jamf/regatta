package key

import (
	"errors"
	"io"
)

type Type byte

const (
	// TypeUnknown unknown Key type.
	TypeUnknown Type = iota
	// TypeUser user Key type.
	TypeUser
	// TypeSystem system/internal Key type.
	TypeSystem
)

const (
	keyHeaderLen        = 4
	keyVersionHeaderPos = 0

	// LatestVersion latest key version implemented.
	LatestVersion = V1
	// LatestVersionLen latest key version maximum length.
	LatestVersionLen = V1KeyLen
	// UnknownVersion unknown key version (versions are numbered from 1).
	UnknownVersion = 0
)

// LatestKeyLen computes the length of key of a latest version.
var LatestKeyLen = V1Len

var LatestMinKey = V1MinKey()

var LatestMaxKey = V1MaxKey()

var (
	// ErrUnknownKeyVersion key version is not implemented in this build.
	ErrUnknownKeyVersion = errors.New("unknown key version")
	// ErrMissingKeyHeader missing header part of the key.
	ErrMissingKeyHeader = errors.New("missing key header")
	// ErrMalformedKeyHeader key header is malformed.
	ErrMalformedKeyHeader = errors.New("malformed key header")
)

// Key generic internal Key (not that field support might be dependant on the stored key version).
type Key struct {
	version uint8
	// KeyType type of a Key - supported since V1.
	KeyType Type
	// Key data part of a Key - supported since V1.
	Key []byte
}

func (k *Key) reset() {
	k.version = UnknownVersion
	k.KeyType = TypeUnknown
	k.Key = k.Key[0:0]
}

// Decoder the Key decoder.
type Decoder struct {
	r io.Reader
}

// NewDecoder constructs a new Decoder.
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

	// Check header padding
	for i := 1; i < len(header); i++ {
		if header[i] != 0x0 {
			return ErrMalformedKeyHeader
		}
	}

	if header[keyVersionHeaderPos] == V1 {
		k := keyV1{}
		err := k.Decode(d.r)
		if err != nil {
			return err
		}
		key.version = V1
		key.KeyType = k.keyType
		key.Key = k.key
		return nil
	}
	return ErrUnknownKeyVersion
}

// Encoder the Key encoder.
type Encoder struct {
	w io.Writer
}

// NewEncoder constructs a new Encoder.
func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{w: writer}
}

func (e Encoder) Encode(key *Key) (int, error) {
	if key.version == UnknownVersion {
		key.version = LatestVersion
	}

	var header [keyHeaderLen]byte
	header[keyVersionHeaderPos] = key.version
	if _, err := e.w.Write(header[:]); err != nil {
		return 0, err
	}

	if key.version == V1 {
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
