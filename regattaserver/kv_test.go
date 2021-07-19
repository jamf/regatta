package regattaserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var kv KVServer

var (
	table1Name   = []byte("table_1")
	table2Name   = []byte("table_2")
	key1Name     = []byte("key_1")
	key2Name     = []byte("key_2")
	key3Name     = []byte("key_3")
	table1Value1 = []byte("table_1/value_1")
	table1Value2 = []byte("table_1/value_2")
	table2Value1 = []byte("table_2/value_1")
	table2Value2 = []byte("table_2/value_2")
)

func TestRegatta_Get(t *testing.T) {
	tests := []struct {
		name           string
		putRequests    []*proto.PutRequest
		rangeRequests  []*proto.RangeRequest
		expectedValues []*proto.RangeResponse
		ms             storage.KVStorage
	}{
		{
			name: "Put new kv",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
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
			name: "Get keys from range",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{Count: 2, Kvs: []*proto.KeyValue{
					{
						Key:   key1Name,
						Value: table1Value1,
					},
					{
						Key:   key2Name,
						Value: table1Value2,
					},
				}},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:    table1Name,
					Key:      key1Name,
					RangeEnd: key3Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
						{
							Key:   key2Name,
							Value: table1Value2,
						},
					},
					Count: 2,
				},
			},
		},
		{
			name: "Get KeysOnly from range",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{Count: 2, Kvs: []*proto.KeyValue{
					{Key: key1Name},
					{Key: key2Name},
				}},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:    table1Name,
					Key:      key1Name,
					RangeEnd: key3Name,
					KeysOnly: true,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{Key: key1Name},
						{Key: key2Name},
					},
					Count: 2,
				},
			},
		},
		{
			name: "Get CountOnly from range",
			ms:   &MockStorage{rangeResponse: proto.RangeResponse{Count: 2}},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:     table1Name,
					Key:       key1Name,
					RangeEnd:  key3Name,
					CountOnly: true,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{Count: 2},
			},
		},
		{
			name: "Get response with rangeEnd set to \\0",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{Count: 3, Kvs: []*proto.KeyValue{
					{
						Key:   key1Name,
						Value: table1Value1,
					},
					{
						Key:   key2Name,
						Value: table1Value2,
					},
					{
						Key:   key3Name,
						Value: table1Value1,
					},
				}},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:    table1Name,
					Key:      key1Name,
					RangeEnd: []byte{0},
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
						{
							Key:   key2Name,
							Value: table1Value2,
						},
						{
							Key:   key3Name,
							Value: table1Value1,
						},
					},
					Count: 3,
				},
			},
		},
		{
			name: "Get response with key and rangeEnd set to \\0",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{Count: 3, Kvs: []*proto.KeyValue{
					{
						Key:   key1Name,
						Value: table1Value1,
					},
					{
						Key:   key2Name,
						Value: table1Value2,
					},
					{
						Key:   key3Name,
						Value: table1Value1,
					},
				}},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:    table1Name,
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
						{
							Key:   key2Name,
							Value: table1Value2,
						},
						{
							Key:   key3Name,
							Value: table1Value1,
						},
					},
					Count: 3,
				},
			},
		},
		{
			name: "Get response with rangeEnd < key",
			ms: &MockStorage{
				rangeResponse: proto.RangeResponse{},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table:    table1Name,
					Key:      key2Name,
					RangeEnd: key1Name,
				},
			},
			expectedValues: []*proto.RangeResponse{{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := require.New(t)
			kv.Storage = test.ms

			t.Log("Get kvs")
			for i, rreq := range test.rangeRequests {
				rresp, err := kv.Range(context.Background(), rreq)
				r.NoError(err, "Failed to get value")
				r.Equal(test.expectedValues[i], rresp)
			}
		})
	}
}

func TestRegatta_Parallel(t *testing.T) {
	kv.Storage = &MockStorage{}
	for i := 0; i < 1000; i++ {
		t.Run("Run parallel reads/writes", func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			t.Log("Reset")
			_, err := ms.Reset(context.Background(), &proto.ResetRequest{})
			r.NoError(err, "Failed to reset")

			t.Log("Put kv")
			_, err = kv.Put(context.Background(), &proto.PutRequest{
				Table: table1Name,
				Key:   key1Name,
				Value: table1Value1,
			})
			r.NoError(err, "Failed to put kv")

			t.Log("Get kv")
			_, err = kv.Range(context.Background(), &proto.RangeRequest{
				Table: table1Name,
				Key:   key1Name,
			})
			if err != nil && err.Error() != status.Errorf(codes.NotFound, "key not found").Error() {
				r.NoError(err, "Failed to get value")
			}
		})
	}
}

func TestRegatta_RangeNotFound(t *testing.T) {
	r := require.New(t)

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
	kv.Storage = &MockStorage{}
	for _, pr := range putRequests {
		_, err := kv.Put(context.Background(), pr)
		r.NoError(err, "Failed to put kv")
	}

	t.Log("Get non-existing kv from existing table")
	kv.Storage = &MockStorage{rangeError: storage.ErrNotFound}
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   []byte("non_existing_key"),
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())

	t.Log("Get kv from non-existing table")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}

func TestRegatta_RangeInvalidArgument(t *testing.T) {
	r := require.New(t)

	t.Log("Get with empty table name")
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Get with empty key name")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Get with negative limit")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
		Limit: -1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "limit must be a positive number").Error())

	t.Log("Get with both CountOnly and KeysOnly")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:     table1Name,
		Key:       key1Name,
		KeysOnly:  true,
		CountOnly: true,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "keys_only and count_only must not be set at the same time").Error())
}

func TestRegatta_RangeUnimplemented(t *testing.T) {
	a := assert.New(t)

	t.Log("Get kv with unimplemented min_mod_revision")
	_, err := kv.Range(context.Background(), &proto.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	_, err = kv.Range(context.Background(), &proto.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestRegatta_PutInvalidArgument(t *testing.T) {
	r := require.New(t)

	t.Log("Put with empty table name")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table: []byte{},
		Key:   key1Name,
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Put with empty key name")
	_, err = kv.Put(context.Background(), &proto.PutRequest{
		Table: table1Name,
		Key:   []byte{},
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Put with managed table name")
	_, err = kv.Put(context.Background(), &proto.PutRequest{
		Table: []byte(managedTable),
		Key:   key1Name,
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table is read-only").Error())
}

func TestRegatta_PutUnimplemented(t *testing.T) {
	r := require.New(t)

	t.Log("Put kv with unimplemented prev_kv")
	_, err := kv.Put(context.Background(), &proto.PutRequest{
		Table:  table1Name,
		Key:    key1Name,
		PrevKv: true,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "prev_kv not implemented").Error())
}

func TestRegatta_DeleteRangeInvalidArgument(t *testing.T) {
	r := require.New(t)

	t.Log("Delete with empty table name")
	_, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Delete with empty key name")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Delete with managed table name")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: []byte(managedTable),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table is read-only").Error())
}

func TestRegatta_DeleteRangeUnimplemented(t *testing.T) {
	a := assert.New(t)

	t.Log("Delete kv with unimplemented range_end")
	_, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table:    table1Name,
		Key:      key1Name,
		RangeEnd: key2Name,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "range_end not implemented").Error())

	t.Log("Delete kv with unimplemented prev_kv")
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table:  table1Name,
		Key:    key1Name,
		PrevKv: true,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "prev_kv not implemented").Error())
}

func TestRegatta_DeleteRange(t *testing.T) {
	r := require.New(t)

	t.Log("Delete existing kv")
	kv.Storage = &MockStorage{deleteRangeResponse: proto.DeleteRangeResponse{Deleted: 1}}
	drresp, err := kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.NoError(err)
	r.Equal(int64(1), drresp.GetDeleted())

	t.Log("Delete non-existing kv")
	kv.Storage = &MockStorage{deleteError: storage.ErrNotFound}
	_, err = kv.DeleteRange(context.Background(), &proto.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "key not found").Error())
}
