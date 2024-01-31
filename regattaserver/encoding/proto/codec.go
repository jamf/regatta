// Copyright JAMF Software, LLC

package proto

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

type vtprotoUnsafeMessage interface {
	UnmarshalVTUnsafe([]byte) error
}

func (Codec) Marshal(v interface{}) ([]byte, error) {
	switch message := v.(type) {
	case vtprotoMessage:
		return message.MarshalVT()
	case proto.Message:
		return proto.Marshal(message)
	default:
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message|vtprotoMessage", v)
	}
}

func (Codec) Unmarshal(data []byte, v interface{}) error {
	switch message := v.(type) {
	case vtprotoUnsafeMessage:
		return message.UnmarshalVTUnsafe(data)
	case vtprotoMessage:
		return message.UnmarshalVT(data)
	case proto.Message:
		return proto.Unmarshal(data, message)
	default:
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message|vtprotoUnsafeMessage|vtprotoMessage", v)
	}
}

func (Codec) Name() string {
	return Name
}
