package regattaserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegatta_Reset(t *testing.T) {
	// TODO: Use proper mock from testify in order to check individual calls on the mock.
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}
	t.Log("Put kv")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table: table1Name,
		Key:   key1Name,
		Value: table1Value1,
	})
	r.NoError(err, "Failed to put kv")

	t.Log("Check kv exists")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.NoError(err, "Failed to get value")

	ms := MaintenanceServer{
		Storage: &MockStorage{},
	}
	t.Log("Reset")
	_, err = ms.Reset(context.Background(), &proto.ResetRequest{})
	r.NoError(err, "Failed to reset")

	t.Log("Check kv doesn't exist")
	kv.Storage = &MockStorage{rangeError: storage.ErrNotFound}
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}

func TestRegatta_Hash(t *testing.T) {
	// TODO: Use proper mock from testify in order to check individual calls on the mock.
	r := require.New(t)
	ms := MaintenanceServer{
		Storage: &MockStorage{},
	}

	t.Log("Reset")
	_, err := ms.Reset(context.Background(), &proto.ResetRequest{})
	r.NoError(err)

	t.Log("Get Hash")
	ms.Storage = &MockStorage{hashResponse: proto.HashResponse{Hash: uint64(5001005189967390176)}}
	hash, err := ms.Hash(context.Background(), &proto.HashRequest{})
	r.NoError(err)

	r.Equal(uint64(5001005189967390176), hash.Hash)
}
