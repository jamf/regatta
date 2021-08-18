package regattaserver

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type Codec struct{}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func (Codec) Marshal(v interface{}) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if !ok {
		vv, ok := v.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message|vtprotoMessage", v)
		}
		return proto.Marshal(vv)
	}
	return vt.MarshalVT()
}

func (Codec) Unmarshal(data []byte, v interface{}) error {
	vt, ok := v.(vtprotoMessage)
	if !ok {
		vv, ok := v.(proto.Message)
		if !ok {
			return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message|vtprotoMessage", v)
		}
		return proto.Unmarshal(data, vv)
	}
	return vt.UnmarshalVT(data)
}

func (Codec) Name() string {
	return Name
}
