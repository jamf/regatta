package regattaserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var kv KVServer

var (
	table1Name           = []byte("table_1")
	table2Name           = []byte("table_2")
	key1Name             = []byte("key_1")
	key2Name             = []byte("key_2")
	table1Value1         = []byte("table_1/value_1")
	table1Value1Modified = []byte("table_1/value_1_modified")
	table1Value2         = []byte("table_1/value_2")
	table2Value1         = []byte("table_2/value_1")
	table2Value2         = []byte("table_2/value_2")
)

func TestRegatta_PutAndGet(t *testing.T) {
	tests := []struct {
		name           string
		putRequests    []*proto.PutRequest
		rangeRequests  []*proto.RangeRequest
		expectedValues []*proto.RangeResponse
	}{
		{
			name: "Put new kv",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
				},
			},
		},
		{
			name: "Rewrite existing kv",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1Modified,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1Modified,
						},
					},
					Count: 1,
				},
			},
		},
		{
			name: "Put more kvs",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
				{
					Table: table1Name,
					Key:   key2Name,
					Value: table1Value2,
				},
				{
					Table: table2Name,
					Key:   key1Name,
					Value: table2Value1,
				},
				{
					Table: table2Name,
					Key:   key2Name,
					Value: table2Value2,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
				{
					Table: table1Name,
					Key:   key2Name,
				},
				{
					Table: table2Name,
					Key:   key1Name,
				},
				{
					Table: table2Name,
					Key:   key2Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key2Name,
							Value: table1Value2,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table2Value1,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key2Name,
							Value: table2Value2,
						},
					},
					Count: 1,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			kv.Storage.Reset()

			t.Log("Put kvs")
			for _, preq := range test.putRequests {
				_, err := kv.Put(context.Background(), preq)
				require.NoError(err, "Failed to put kv")
			}

			t.Log("Get kvs")
			for i, rreq := range test.rangeRequests {
				rresp, err := kv.Range(context.Background(), rreq)
				require.NoError(err, "Failed to get value")
				require.Equal(test.expectedValues[i], rresp)
			}
		})
	}
}

func TestRegatta_Parallel(t *testing.T) {
	for i := 0; i < 1000; i++ {
		t.Run("Run parallel reads/writes", func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			t.Log("Reset")
			_, err := ms.Reset(context.Background(), &proto.ResetRequest{})
			require.NoError(err, "Failed to reset")

			t.Log("Put kv")
			_, err = kv.Put(context.Background(), &proto.PutRequest{
				Table: table1Name,
				Key:   key1Name,
				Value: table1Value1,
			})
			require.NoError(err, "Failed to put kv")

			t.Log("Get kv")
			_, err = kv.Range(context.Background(), &proto.RangeRequest{
				Table: table1Name,
				Key:   key1Name,
			})
			if err != nil && err.Error() != status.Errorf(codes.NotFound, "key not found").Error() {
				require.NoError(err, "Failed to get value")
			}
		})
	}
}

func TestRegatta_RangeNotFound(t *testing.T) {
	require := require.New(t)

	putRequests := []*proto.PutRequest{
		{
			Table: table1Name,
			Key:   key1Name,
			Value: table1Value1,
		},
		{
			Table: table1Name,
			Key:   key2Name,
			Value: table1Value2,
		},
		{
			Table: table2Name,
			Key:   key1Name,
			Value: table2Value1,
		},
		{
			Table: table2Name,
			Key:   key2Name,
			Value: table2Value2,
		},
	}

	t.Log("Put initial data")
	for _, pr := range putRequests {
		_, err := kv.Put(context.Background(), pr)
		require.NoError(err, "Failed to put kv")
	}

	t.Log("Get non-existing kv from existing table")
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   []byte("non_existing_key"),
	})
	require.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())

	t.Log("Get kv from non-existing table")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}

func TestRegatta_RangeInvalidArgument(t *testing.T) {
	require := require.New(t)

	t.Log("Get with empty table name")
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Get with empty key name")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())
}

func TestRegatta_RangeUnimplemented(t *testing.T) {
	assert := assert.New(t)

	t.Log("Get kv with unimplemented range_end")
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table:    table1Name,
		Key:      key1Name,
		RangeEnd: key2Name,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "range_end not implemented").Error())

	t.Log("Get kv with unimplemented limit")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
		Limit: 1,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "limit not implemented").Error())

	t.Log("Get kv with unimplemented linearizable")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:        table1Name,
		Key:          key1Name,
		Linearizable: true,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "linearizable not implemented").Error())

	t.Log("Get kv with unimplemented keys_only")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:    table1Name,
		Key:      key1Name,
		KeysOnly: true,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "keys_only not implemented").Error())

	t.Log("Get kv with unimplemented count_only")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:     table1Name,
		Key:       key1Name,
		CountOnly: true,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "count_only not implemented").Error())

	t.Log("Get kv with unimplemented min_mod_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestRegatta_PutInvalidArgument(t *testing.T) {
	require := require.New(t)

	t.Log("Put with empty table name")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table: []byte{},
		Key:   key1Name,
		Value: table1Value1,
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Put with empty key name")
	_, err = kv.Put(context.Background(), &proto.PutRequest{
		Table: table1Name,
		Key:   []byte{},
		Value: table1Value1,
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())
}

func TestRegatta_PutUnimplemented(t *testing.T) {
	require := require.New(t)

	t.Log("Put kv with unimplemented prev_kv")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table:  table1Name,
		Key:    key1Name,
		PrevKv: true,
	})
	require.EqualError(err, status.Errorf(codes.Unimplemented, "prev_kv not implemented").Error())
}

func TestRegatta_DeleteRangeInvalidArgument(t *testing.T) {
	require := require.New(t)

	t.Log("Delete with empty table name")
	_, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Delete with empty key name")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	require.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())
}

func TestRegatta_DeleteRangeUnimplemented(t *testing.T) {
	assert := assert.New(t)

	t.Log("Delete kv with unimplemented range_end")
	_, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table:    table1Name,
		Key:      key1Name,
		RangeEnd: key2Name,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "range_end not implemented").Error())

	t.Log("Delete kv with unimplemented prev_kv")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table:  table1Name,
		Key:    key1Name,
		PrevKv: true,
	})
	assert.EqualError(err, status.Errorf(codes.Unimplemented, "prev_kv not implemented").Error())
}

func TestRegatta_DeleteRange(t *testing.T) {
	require := require.New(t)

	t.Log("Put kv")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table: table1Name,
		Key:   key1Name,
		Value: table1Value1,
	})
	require.NoError(err, "Failed to put kv")

	t.Log("Delete existing kv")
	drresp, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	require.NoError(err)
	require.Equal(int64(1), drresp.GetDeleted())

	t.Log("Get non-existing kv")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())

	t.Log("Delete non-existing kv")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	require.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}
