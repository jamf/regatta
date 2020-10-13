package storage

import (
	"context"
	"errors"

	"github.com/wandera/regatta/proto"
)

// ErrNotFound returned when the key is not found.
var ErrNotFound = errors.New("key not found")

type KVStorage interface {
	Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error)
	Put(ctx context.Context, req *proto.PutRequest) (Result, error)
	Delete(ctx context.Context, req *proto.DeleteRangeRequest) (Result, error)
	Reset(ctx context.Context, req *proto.ResetRequest) error
	Hash(ctx context.Context, req *proto.HashRequest) (uint64, error)
}

type Result struct {
	// Value is a 64 bits integer value used to indicate the outcome of the
	// update operation.
	Value uint64
	// Data is an optional byte slice created and returned by the update
	// operation. It is useful for CompareAndSwap style updates in which an
	// arbitrary length of bytes need to be returned.
	// Users are strongly recommended to use the query methods supported by
	// NodeHost to query the state of their IStateMachine and IOnDiskStateMachine
	// types, proposal based queries are known to work but are not recommended.
	Data []byte
}
