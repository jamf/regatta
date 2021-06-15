package key

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecoder_Decode(t *testing.T) {
	type fields struct {
		r io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		wantKey Key
	}{
		{
			name:   "Decode - V1 System key",
			fields: fields{r: bytes.NewBuffer(append([]byte{V1, 0x0, 0x0, 0x0, byte(TypeSystem)}, []byte("test")...))},
			wantKey: Key{
				version: V1,
				KeyType: TypeSystem,
				Key:     []byte("test"),
			},
		},
		{
			name:   "Decode - V1 User key",
			fields: fields{r: bytes.NewBuffer(append([]byte{V1, 0x0, 0x0, 0x0, byte(TypeUser)}, []byte("test")...))},
			wantKey: Key{
				version: V1,
				KeyType: TypeUser,
				Key:     []byte("test"),
			},
		},
		{
			name:    "Decode - Malformed header",
			fields:  fields{r: bytes.NewBuffer(append([]byte{0x0, 0x0, byte(TypeUser)}, []byte("test")...))},
			wantErr: true,
		},
		{
			name:    "Decode - Missing header",
			fields:  fields{r: bytes.NewBuffer([]byte{0x0, 0x0})},
			wantErr: true,
		},
		{
			name:    "Decode - Unknown Key Version",
			fields:  fields{r: bytes.NewBuffer(append([]byte{UnknownVersion, 0x0, 0x0, 0x0, byte(TypeUser)}, []byte("test")...))},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			d := NewDecoder(tt.fields.r)

			k := Key{}
			err := d.Decode(&k)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.wantKey, k)
		})
	}
}

func TestEncoder_Encode(t *testing.T) {
	type fields struct {
		w io.Writer
	}
	type args struct {
		key *Key
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantErr    bool
		wantWriter []byte
	}{
		{
			name:   "Encode - V1 System key",
			fields: fields{w: bytes.NewBuffer(make([]byte, 0, V1KeyLen))},
			args: args{key: &Key{
				version: V1,
				KeyType: TypeSystem,
				Key:     []byte("test"),
			}},
			wantWriter: append([]byte{V1, 0x0, 0x0, 0x0, byte(TypeSystem)}, []byte("test")...),
		},
		{
			name:   "Encode - V1 User key",
			fields: fields{w: bytes.NewBuffer(make([]byte, 0, V1KeyLen))},
			args: args{key: &Key{
				version: V1,
				KeyType: TypeUser,
				Key:     []byte("test"),
			}},
			wantWriter: append([]byte{V1, 0x0, 0x0, 0x0, byte(TypeUser)}, []byte("test")...),
		},
		{
			name:   "Encode - V1 System key - small buffer",
			fields: fields{w: bytes.NewBuffer(make([]byte, 0, 1))},
			args: args{key: &Key{
				version: V1,
				KeyType: TypeUser,
				Key:     []byte("test"),
			}},
			wantWriter: append([]byte{V1, 0x0, 0x0, 0x0, byte(TypeUser)}, []byte("test")...),
		},
		{
			name:   "Encode - Auto-pick Latest Key Version",
			fields: fields{w: bytes.NewBuffer(make([]byte, 0, 1))},
			args: args{key: &Key{
				KeyType: TypeUser,
				Key:     []byte("test"),
			}},
			wantWriter: append([]byte{LatestVersion, 0x0, 0x0, 0x0, byte(TypeUser)}, []byte("test")...),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			e := NewEncoder(tt.fields.w)

			n, err := e.Encode(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.wantWriter, tt.fields.w.(*bytes.Buffer).Bytes()[:n])
		})
	}
}

func TestKey_reset(t *testing.T) {
	type fields struct {
		version uint8
		KeyType Type
		Key     []byte
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "Reset - V1 System key",
			fields: fields{
				version: V1,
				KeyType: TypeSystem,
				Key:     []byte("test"),
			},
		},
		{
			name: "Reset - V1 User key",
			fields: fields{
				version: V1,
				KeyType: TypeUser,
				Key:     []byte("test"),
			},
		},
		{
			name:   "Reset - Empty key",
			fields: fields{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			k := &Key{
				version: tt.fields.version,
				KeyType: tt.fields.KeyType,
				Key:     tt.fields.Key,
			}
			k.reset()

			r.Empty(k.version)
			r.Empty(k.KeyType)
			r.Empty(k.Key)
		})
	}
}
