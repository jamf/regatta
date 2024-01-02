// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/util/iter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

func TestKVServer_Range(t *testing.T) {
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

func TestKVServer_RangeError(t *testing.T) {
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

	t.Log("Get unknown error")
	kv = KVServer{
		Storage: &MockStorage{rangeError: fmt.Errorf("unknown")},
	}
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Internal, "unknown").Error())
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

func TestKVServer_IterateRangeError(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Get with empty table name")
	err := kv.IterateRange(&regattapb.RangeRequest{
		Table: []byte{},
		Key:   key1Name,
	}, nil)
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Get with empty key name")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte{},
	}, nil)
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "key must be set").Error())

	t.Log("Get with negative limit")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
		Limit: -1,
	}, nil)
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "limit must be a positive number").Error())

	t.Log("Get with both CountOnly and KeysOnly")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:     table1Name,
		Key:       key1Name,
		KeysOnly:  true,
		CountOnly: true,
	}, nil)
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "keys_only and count_only must not be set at the same time").Error())

	t.Log("Get kv from non-existing table")
	kv.Storage = &MockStorage{iterateRangeError: errors.ErrTableNotFound}
	srv := &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	}, srv)
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Get unknown error")
	kv.Storage = &MockStorage{iterateRangeError: fmt.Errorf("unknown")}
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	}, srv)
	r.EqualError(err, status.Errorf(codes.Internal, "unknown").Error())

	t.Log("Get unknown send error")
	kv.Storage = &MockStorage{iterateRangeResponse: iter.From(&regattapb.RangeResponse{})}

	srv.On("Send", mock.Anything).Return(fmt.Errorf("uknown send error"))
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	}, srv)
	r.EqualError(err, status.Errorf(codes.Internal, "uknown send error").Error())
}

func TestKVServer_IterateRangeUnimplemented(t *testing.T) {
	a := assert.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Get kv with unimplemented min_mod_revision")
	err := kv.IterateRange(&regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	}, nil)
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	}, nil)
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	}, nil)
	a.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	}, nil)
	a.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestKVServer_IterateRange(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("IterateRange single message")
	kv.Storage = &MockStorage{iterateRangeResponse: iter.From(&regattapb.RangeResponse{})}
	srv := &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	srv.On("Send", mock.AnythingOfType("*regattapb.RangeResponse")).Return(nil).Once()
	err := kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}, srv)
	r.NoError(err)

	t.Log("IterateRange multi messages")
	kv.Storage = &MockStorage{iterateRangeResponse: iter.From(&regattapb.RangeResponse{}, &regattapb.RangeResponse{}, &regattapb.RangeResponse{})}
	srv = &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	srv.On("Send", mock.AnythingOfType("*regattapb.RangeResponse")).Return(nil).Times(3)
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}, srv)
	r.NoError(err)

	t.Log("IterateRange canceled context")
	kv.Storage = &MockStorage{iterateRangeResponse: iter.From(&regattapb.RangeResponse{}, &regattapb.RangeResponse{}, &regattapb.RangeResponse{})}
	srv = &mockIterateRangeServer{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	srv.On("Context").Return(ctx)
	srv.On("Send", mock.AnythingOfType("*regattapb.RangeResponse")).Return(nil).Times(0)
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}, srv)
	r.NoError(err)
}

func TestKVServer_PutError(t *testing.T) {
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

	t.Log("Put unknown error")
	kv.Storage = &MockStorage{putError: fmt.Errorf("unknown")}
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Internal, "unknown").Error())
}

func TestKVServer_DeleteRangeError(t *testing.T) {
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

	t.Log("Delete unknown error")
	kv.Storage = &MockStorage{deleteError: fmt.Errorf("unknown")}
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Internal, "unknown").Error())
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

func TestKVServer_TxnError(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Txn with empty table name")
	_, err := kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Txn with non-existing table")
	kv.Storage = &MockStorage{txnError: errors.ErrTableNotFound}
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: []byte("non_existing_table"),
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Txn unknown error")
	kv.Storage = &MockStorage{txnError: fmt.Errorf("unknown")}
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: table1Name,
	})
	r.EqualError(err, status.Errorf(codes.Internal, "unknown").Error())
}

func TestKVServer_Txn(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &MockStorage{},
	}

	t.Log("Txn with correct params")
	_, err := kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: table1Name,
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestRange{
					RequestRange: &regattapb.RequestOp_Range{},
				},
			},
		},
	})
	r.NoError(err)
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

type mockIterateRangeServer struct {
	mock.Mock
}

func (m *mockIterateRangeServer) Send(response *regattapb.RangeResponse) error {
	return m.Mock.Called(response).Error(0)
}

func (m *mockIterateRangeServer) SetHeader(md metadata.MD) error {
	return m.Mock.Called(md).Error(0)
}

func (m *mockIterateRangeServer) SendHeader(md metadata.MD) error {
	return m.Mock.Called(md).Error(0)
}

func (m *mockIterateRangeServer) SetTrailer(md metadata.MD) {
	m.Mock.Called(md)
}

func (m *mockIterateRangeServer) Context() context.Context {
	return m.Mock.Called().Get(0).(context.Context)
}

func (m *mockIterateRangeServer) SendMsg(mes any) error {
	return m.Mock.Called(mes).Error(0)
}

func (m *mockIterateRangeServer) RecvMsg(mes any) error {
	return m.Mock.Called(mes).Error(0)
}
