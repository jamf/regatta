package key

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_keyV1_Decode(t *testing.T) {
	type args struct {
		reader io.Reader
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantKey keyV1
	}{
		{
			name: "Decode System key",
			args: args{reader: bytes.NewBuffer(append([]byte{byte(TypeSystem)}, []byte("test")...))},
			wantKey: keyV1{
				keyType: TypeSystem,
				key:     []byte("test"),
			},
		},
		{
			name: "Decode User key",
			args: args{reader: bytes.NewBuffer(append([]byte{byte(TypeUser)}, []byte("test")...))},
			wantKey: keyV1{
				keyType: TypeUser,
				key:     []byte("test"),
			},
		},
		{
			name: "Decode Empty System key",
			args: args{reader: bytes.NewBuffer([]byte{byte(TypeSystem)})},
			wantKey: keyV1{
				keyType: TypeSystem,
				key:     []byte{},
			},
		},
		{
			name: "Decode Empty User key",
			args: args{reader: bytes.NewBuffer([]byte{byte(TypeUser)})},
			wantKey: keyV1{
				keyType: TypeUser,
				key:     []byte{},
			},
		},
		{
			name:    "Decode Missing Key type",
			args:    args{reader: bytes.NewBuffer([]byte{})},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			k := keyV1{}
			err := k.Decode(tt.args.reader)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.wantKey, k)
		})
	}
}

func Test_keyV1_Encode(t *testing.T) {
	type fields struct {
		keyType Type
		key     []byte
	}
	tests := []struct {
		name       string
		fields     fields
		wantWriter []byte
		wantErr    bool
	}{
		{
			name: "Encode System key",
			fields: fields{
				keyType: TypeSystem,
				key:     []byte("test"),
			},
			wantWriter: append([]byte{byte(TypeSystem)}, []byte("test")...),
		},
		{
			name: "Encode User key",
			fields: fields{
				keyType: TypeUser,
				key:     []byte("test"),
			},
			wantWriter: append([]byte{byte(TypeUser)}, []byte("test")...),
		},
		{
			name: "Encode Empty System key",
			fields: fields{
				keyType: TypeSystem,
				key:     []byte{},
			},
			wantWriter: []byte{byte(TypeSystem)},
		},
		{
			name: "Encode Empty User key",
			fields: fields{
				keyType: TypeUser,
				key:     []byte{},
			},
			wantWriter: []byte{byte(TypeUser)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			k := keyV1{
				keyType: tt.fields.keyType,
				key:     tt.fields.key,
			}
			writer := &bytes.Buffer{}
			_, err := k.Encode(writer)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			gotWriter := writer.Bytes()
			r.Equal(tt.wantWriter, gotWriter)
		})
	}
}
