package regattaserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ms MaintenanceServer

func TestRegatta_Reset(t *testing.T) {
	var err error
	require := require.New(t)

	t.Log("Put kv")
	_, err = kv.Put(context.Background(), &proto.PutRequest{
		Table: table1Name,
		Key:   key1Name,
		Value: table1Value1,
	})
	require.NoError(err, "Failed to put kv")

	t.Log("Check kv exists")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	require.NoError(err, "Failed to get value")

	t.Log("Reset")
	_, err = ms.Reset(context.Background(), &proto.ResetRequest{})
	require.NoError(err, "Failed to reset")

	t.Log("Check kv doesn't exist")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}

func TestRegatta_HashUnimplemented(t *testing.T) {
	var err error
	require := require.New(t)

	t.Log("Get hash")
	_, err = ms.Hash(context.Background(), &proto.HashRequest{})
	require.EqualError(err, status.Errorf(codes.Unimplemented, "method Hash not implemented").Error())
}