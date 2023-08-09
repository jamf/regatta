// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"testing"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

func TestKVServer_Get(t *testing.T) {
	tests := []struct {
		name          string
		rangeRequest  *regattapb.RangeRequest
		expectedValue *regattapb.RangeResponse
		ms            KVService
	}{
		{
			name: "Get one key without rangeEnd",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{
					Kvs: []*regattapb.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
				},
			},
			rangeRequest: &regattapb.RangeRequest{
				Table: table1Name,
				Key:   key1Name,
			},
			expectedValue: &regattapb.RangeResponse{
				Kvs: []*regattapb.KeyValue{
					{
						Key:   key1Name,
						Value: table1Value1,
					},
				},
				Count: 1,
			},
		},
		{
			name: "Get keys from range",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{Count: 2, Kvs: []*regattapb.KeyValue{
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
			rangeRequest: &regattapb.RangeRequest{
				Table:    table1Name,
				Key:      key1Name,
				RangeEnd: key3Name,
			},
			expectedValue: &regattapb.RangeResponse{
				Kvs: []*regattapb.KeyValue{
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
		{
			name: "Get KeysOnly from range",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{Count: 2, Kvs: []*regattapb.KeyValue{
					{Key: key1Name},
					{Key: key2Name},
				}},
			},
			rangeRequest: &regattapb.RangeRequest{
				Table:    table1Name,
				Key:      key1Name,
				RangeEnd: key3Name,
				KeysOnly: true,
			},
			expectedValue: &regattapb.RangeResponse{
				Kvs: []*regattapb.KeyValue{
					{Key: key1Name},
					{Key: key2Name},
				},
				Count: 2,
			},
		},
		{
			name: "Get CountOnly from range",
			ms:   &MockStorage{rangeResponse: regattapb.RangeResponse{Count: 2}},
			rangeRequest: &regattapb.RangeRequest{
				Table:     table1Name,
				Key:       key1Name,
				RangeEnd:  key3Name,
				CountOnly: true,
			},
			expectedValue: &regattapb.RangeResponse{
				Count: 2,
			},
		},
		{
			name: "Get response with rangeEnd set to \\0",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{Count: 3, Kvs: []*regattapb.KeyValue{
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
			rangeRequest: &regattapb.RangeRequest{
				Table:    table1Name,
				Key:      key1Name,
				RangeEnd: []byte{0},
			},
			expectedValue: &regattapb.RangeResponse{
				Kvs: []*regattapb.KeyValue{
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
		{
			name: "Get response with key and rangeEnd set to \\0",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{Count: 3, Kvs: []*regattapb.KeyValue{
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
			rangeRequest: &regattapb.RangeRequest{
				Table:    table1Name,
				Key:      []byte{0},
				RangeEnd: []byte{0},
			},
			expectedValue: &regattapb.RangeResponse{
				Kvs: []*regattapb.KeyValue{
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
		{
			name: "Get response with rangeEnd < key",
			ms: &MockStorage{
				rangeResponse: regattapb.RangeResponse{},
			},
			rangeRequest: &regattapb.RangeRequest{
				Table:    table1Name,
				Key:      key2Name,
				RangeEnd: key1Name,
			},
			expectedValue: &regattapb.RangeResponse{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := require.New(t)
			kv := KVServer{
				Storage: test.ms,
			}

			t.Log(test.name)
			rresp, err := kv.Range(context.Background(), test.rangeRequest)
			r.NoError(err, "Failed to get value")
			r.Equal(test.expectedValue, rresp)
		})
	}
}

func TestKVServer_Parallel(t *testing.T) {
	kv := KVServer{Storage: &MockStorage{}}
	for i := 0; i < 100; i++ {
		t.Run("Run parallel reads/writes", func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			t.Log("Put kv")
			_, err := kv.Put(context.Background(), &regattapb.PutRequest{
				Table: table1Name,
				Key:   key1Name,
				Value: table1Value1,
			})
			r.NoError(err, "Failed to put kv")

			t.Log("Get kv")
			_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
				Table: table1Name,
				Key:   key1Name,
			})
			if err != nil && err.Error() != status.Errorf(codes.NotFound, "key not found").Error() {
				r.NoError(err, "Failed to get value")
			}
		})
	}
}

func TestKVServer_RangeNotFound(t *testing.T) {
	r := require.New(t)

	putRequests := []*regattapb.PutRequest{
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

	kv := KVServer{
		Storage: &MockStorage{},
	}
	for _, pr := range putRequests {
		_, err := kv.Put(context.Background(), pr)
		r.NoError(err, "Failed to put kv")
	}

	t.Log("Get non-existing kv from existing table")
	kv.Storage = &MockStorage{rangeResponse: regattapb.RangeResponse{}}
	_, err := kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("non_existing_key"),
	})
	r.NoError(err)

	t.Log("Get kv from non-existing table")
	kv.Storage = &MockStorage{rangeError: errors.ErrTableNotFound}
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())
}

func TestKVServer_RangeInvalidArgument(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Get with empty table name")
	_, err := kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Get with empty key name")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Get with negative limit")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
		Limit: -1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "limit must be a positive number").Error())

	t.Log("Get with both CountOnly and KeysOnly")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:     table1Name,
		Key:       key1Name,
		KeysOnly:  true,
		CountOnly: true,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "keys_only and count_only must not be set at the same time").Error())
}

func TestKVServer_RangeUnimplemented(t *testing.T) {
	a := assert.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Get kv with unimplemented min_mod_revision")
	_, err := kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	})
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestKVServer_PutInvalidArgument(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Put with empty table name")
	_, err := kv.Put(context.Background(), &regattapb.PutRequest{
		Table: []byte{},
		Key:   key1Name,
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Put with empty key name")
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   []byte{},
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Put with non-existing table")
	kv.Storage = &MockStorage{putError: errors.ErrTableNotFound}
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())
}

func TestKVServer_DeleteRangeInvalidArgument(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Delete with empty table name")
	_, err := kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: []byte{},
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Delete with empty key name")
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Delete with non-existing table")
	kv.Storage = &MockStorage{deleteError: errors.ErrTableNotFound}
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())
}

func TestKVServer_DeleteRange(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Delete existing kv")
	kv.Storage = &MockStorage{deleteRangeResponse: regattapb.DeleteRangeResponse{Deleted: 1}}
	drresp, err := kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.NoError(err)
	r.Equal(int64(1), drresp.GetDeleted())
}

func TestReadonlyKVServer_Put(t *testing.T) {
	r := require.New(t)
	kv := ReadonlyKVServer{
		KVServer: KVServer{
			Storage: &MockStorage{},
		},
	}

	t.Log("Put kv")
	_, err := kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "method Put not implemented for follower").Error())
}

func TestReadonlyKVServer_DeleteRange(t *testing.T) {
	r := require.New(t)
	kv := ReadonlyKVServer{
		KVServer: KVServer{
			Storage: &MockStorage{},
		},
	}

	t.Log("Delete existing kv")
	_, err := kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "method DeleteRange not implemented for follower").Error())
}

func TestReadonlyKVServer_Txn(t *testing.T) {
	r := require.New(t)
	kv := ReadonlyKVServer{
		KVServer: KVServer{
			Storage: &MockStorage{},
		},
	}

	t.Log("Writable Txn")
	_, err := kv.Txn(context.Background(), &regattapb.TxnRequest{
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestPut{RequestPut: &regattapb.RequestOp_Put{
					Key: key1Name,
				}},
			},
		},
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "writable Txn not implemented for follower").Error())

	t.Log("Readonly Txn")
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: table1Name,
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestRange{RequestRange: &regattapb.RequestOp_Range{
					Key: key1Name,
				}},
			},
		},
	})
	r.NoError(err)
}
