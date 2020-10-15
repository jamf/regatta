package storage

import (
	"context"
	"errors"

	"github.com/wandera/regatta/proto"
)

var (
	// ErrNotFound returned when the key is not found.
	ErrNotFound = errors.New("key not found")
	// ErrEmptyKey returned when the key is not provided.
	ErrEmptyKey = errors.New("key must not be empty")
	// ErrEmptyTable returned when the table is not provided.
	ErrEmptyTable = errors.New("table must not be empty")
)

type KVStorage interface {
	Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error)
	Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error)
	Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error)
	Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error)
	Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error)
}
