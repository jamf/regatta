// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/util/iter"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	table1Name   = []byte("table_1")
	key1Name     = []byte("key_1")
	table1Value1 = []byte("table_1/value_1")
)

func TestKVServer_Range(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("Get key")
	storage.On("Range", mock.Anything, &regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}).Return(&regattapb.RangeResponse{}, nil)
	_, err := kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})

	r.NoError(err)
}

func TestKVServer_RangeError(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
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

	t.Log("Get kv from non-existing table")
	storage = &mockKVService{}
	storage.On("Range", mock.Anything, mock.Anything).Return((*regattapb.RangeResponse)(nil), errors.ErrTableNotFound)
	kv.Storage = storage
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Get unknown error")
	storage = &mockKVService{}
	storage.On("Range", mock.Anything, mock.Anything).Return((*regattapb.RangeResponse)(nil), fmt.Errorf("unknown"))
	kv.Storage = storage
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.FailedPrecondition, "unknown").Error())

	t.Log("Get retry-safe error")
	storage = &mockKVService{}
	storage.On("Range", mock.Anything, mock.Anything).Return((*regattapb.RangeResponse)(nil), raft.ErrSystemBusy)
	kv.Storage = storage
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Unavailable, raft.ErrSystemBusy.Error()).Error())
}

func TestKVServer_RangeUnimplemented(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &mockKVService{},
	}

	t.Log("Get kv with unimplemented min_mod_revision")
	_, err := kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	_, err = kv.Range(context.Background(), &regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	})
	r.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestKVServer_IterateRangeError(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
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
	storage = &mockKVService{}
	storage.On("IterateRange", mock.Anything, mock.Anything).Return((iter.Seq[*regattapb.RangeResponse])(nil), errors.ErrTableNotFound)
	kv.Storage = storage
	srv := &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	}, srv)
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Get unknown error")
	storage = &mockKVService{}
	storage.On("IterateRange", mock.Anything, mock.Anything).Return((iter.Seq[*regattapb.RangeResponse])(nil), fmt.Errorf("unknown"))
	kv.Storage = storage
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	}, srv)
	r.EqualError(err, status.Errorf(codes.FailedPrecondition, "unknown").Error())

	t.Log("Get unknown send error")
	storage = &mockKVService{}
	storage.On("IterateRange", mock.Anything, mock.Anything).Return(iter.From(&regattapb.RangeResponse{}), nil)
	kv.Storage = storage

	srv.On("Send", mock.Anything).Return(fmt.Errorf("uknown send error"))
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	}, srv)
	r.EqualError(err, status.Errorf(codes.Internal, "uknown send error").Error())

	t.Log("Get retry-safe error")
	storage = &mockKVService{}
	storage.On("IterateRange", mock.Anything, mock.Anything).Return((iter.Seq[*regattapb.RangeResponse])(nil), raft.ErrSystemBusy)
	kv.Storage = storage
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	}, srv)
	r.EqualError(err, status.Errorf(codes.Unavailable, raft.ErrSystemBusy.Error()).Error())
}

func TestKVServer_IterateRangeUnimplemented(t *testing.T) {
	r := require.New(t)
	kv := KVServer{
		Storage: &mockKVService{},
	}

	t.Log("Get kv with unimplemented min_mod_revision")
	err := kv.IterateRange(&regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MinModRevision: 1,
	}, nil)
	r.EqualError(err, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_mod_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:          table1Name,
		Key:            key1Name,
		MaxModRevision: 1,
	}, nil)
	r.EqualError(err, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented").Error())

	t.Log("Get kv with unimplemented min_create_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MinCreateRevision: 1,
	}, nil)
	r.EqualError(err, status.Errorf(codes.Unimplemented, "min_create_revision not implemented").Error())

	t.Log("Get kv with unimplemented max_create_revision")
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table:             table1Name,
		Key:               key1Name,
		MaxCreateRevision: 1,
	}, nil)
	r.EqualError(err, status.Errorf(codes.Unimplemented, "max_create_revision not implemented").Error())
}

func TestKVServer_IterateRange(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("IterateRange single message")
	storage.On("IterateRange", mock.Anything, mock.Anything).Return(iter.From(&regattapb.RangeResponse{}), nil)
	srv := &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	srv.On("Send", mock.AnythingOfType("*regattapb.RangeResponse")).Return(nil).Once()
	err := kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}, srv)
	r.NoError(err)

	t.Log("IterateRange multi messages")
	storage.On("IterateRange", mock.Anything, mock.Anything).Return(iter.From(&regattapb.RangeResponse{}, &regattapb.RangeResponse{}, &regattapb.RangeResponse{}))
	srv = &mockIterateRangeServer{}
	srv.On("Context").Return(context.Background())
	srv.On("Send", mock.AnythingOfType("*regattapb.RangeResponse")).Return(nil).Times(3)
	err = kv.IterateRange(&regattapb.RangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}, srv)
	r.NoError(err)

	t.Log("IterateRange canceled context")
	storage.On("IterateRange", mock.Anything, mock.Anything).Return(iter.From(&regattapb.RangeResponse{}, &regattapb.RangeResponse{}, &regattapb.RangeResponse{}))
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
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
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
	storage.On("Put", mock.Anything, mock.Anything).Return((*regattapb.PutResponse)(nil), errors.ErrTableNotFound)
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
		Value: table1Value1,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Put unknown error")
	storage = &mockKVService{}
	storage.On("Put", mock.Anything, mock.Anything).Return((*regattapb.PutResponse)(nil), fmt.Errorf("unknown"))
	kv.Storage = storage
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.FailedPrecondition, "unknown").Error())

	t.Log("Put retry-safe error")
	storage = &mockKVService{}
	storage.On("Put", mock.Anything, mock.Anything).Return((*regattapb.PutResponse)(nil), raft.ErrSystemBusy)
	kv.Storage = storage
	_, err = kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Unavailable, raft.ErrSystemBusy.Error()).Error())
}

func TestKVServer_Put(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("Put new kv")
	storage.On("Put", mock.Anything, &regattapb.PutRequest{
		Table: table1Name,
		Key:   key1Name,
		Value: table1Value1,
	}).Return(&regattapb.PutResponse{}, nil)
	_, err := kv.Put(context.Background(), &regattapb.PutRequest{
		Table: table1Name,
		Key:   key1Name,
		Value: table1Value1,
	})
	r.NoError(err)
}

func TestKVServer_DeleteRangeError(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
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
	storage.On("Delete", mock.Anything, mock.Anything).Return((*regattapb.DeleteRangeResponse)(nil), errors.ErrTableNotFound)
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: []byte("non_existing_table"),
		Key:   key1Name,
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Delete unknown error")
	storage = &mockKVService{}
	storage.On("Delete", mock.Anything, mock.Anything).Return((*regattapb.DeleteRangeResponse)(nil), fmt.Errorf("unknown"))
	kv.Storage = storage
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.FailedPrecondition, "unknown").Error())

	t.Log("Delete retry-safe error")
	storage = &mockKVService{}
	storage.On("Delete", mock.Anything, mock.Anything).Return((*regattapb.DeleteRangeResponse)(nil), raft.ErrSystemBusy)
	kv.Storage = storage
	_, err = kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   []byte("foo"),
	})
	r.EqualError(err, status.Errorf(codes.Unavailable, raft.ErrSystemBusy.Error()).Error())
}

func TestKVServer_DeleteRange(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("Delete existing kv")
	storage.On("Delete", mock.Anything, &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}).Return(&regattapb.DeleteRangeResponse{Deleted: 1}, nil)
	drresp, err := kv.DeleteRange(context.Background(), &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	})
	r.NoError(err)
	r.Equal(int64(1), drresp.GetDeleted())
}

func TestKVServer_TxnError(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("Txn with empty table name")
	_, err := kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: []byte{},
	})
	r.EqualError(err, status.Errorf(codes.InvalidArgument, "table must be set").Error())

	t.Log("Txn with non-existing table")
	storage.On("Txn", mock.Anything, mock.Anything).Return((*regattapb.TxnResponse)(nil), errors.ErrTableNotFound)
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: []byte("non_existing_table"),
	})
	r.EqualError(err, status.Errorf(codes.NotFound, "table not found").Error())

	t.Log("Txn unknown error")
	storage = &mockKVService{}
	storage.On("Txn", mock.Anything, mock.Anything).Return((*regattapb.TxnResponse)(nil), fmt.Errorf("unknown"))
	kv.Storage = storage
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: table1Name,
	})
	r.EqualError(err, status.Errorf(codes.FailedPrecondition, "unknown").Error())

	t.Log("Txn retry-safe error")
	storage = &mockKVService{}
	storage.On("Txn", mock.Anything, mock.Anything).Return((*regattapb.TxnResponse)(nil), raft.ErrSystemBusy)
	kv.Storage = storage
	_, err = kv.Txn(context.Background(), &regattapb.TxnRequest{
		Table: table1Name,
	})
	r.EqualError(err, status.Errorf(codes.Unavailable, raft.ErrSystemBusy.Error()).Error())
}

func TestKVServer_Txn(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	kv := KVServer{
		Storage: storage,
	}

	t.Log("Txn with correct params")
	storage.On("Txn", mock.Anything, &regattapb.TxnRequest{
		Table: table1Name,
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestRange{
					RequestRange: &regattapb.RequestOp_Range{},
				},
			},
		},
	}).Return(&regattapb.TxnResponse{}, nil)
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

func TestForwardingKVServer_Put(t *testing.T) {
	r := require.New(t)
	client := &mockClient{}
	kv := ForwardingKVServer{
		KVServer: KVServer{
			Storage: &mockKVService{},
		},
		client: client,
		q:      fakeQueue{},
	}
	ctx := context.Background()
	req := &regattapb.PutRequest{
		Table: table1Name,
		Key:   key1Name,
	}
	client.On("Put", ctx, req, mock.Anything).Return(&regattapb.PutResponse{Header: &regattapb.ResponseHeader{Revision: 1}}, nil)
	t.Log("Put kv")
	resp, err := kv.Put(ctx, req)
	r.NoError(err)
	r.Equal(uint64(1), resp.Header.Revision)
}

func TestForwardingKVServer_DeleteRange(t *testing.T) {
	r := require.New(t)
	client := &mockClient{}
	kv := ForwardingKVServer{
		KVServer: KVServer{
			Storage: &mockKVService{},
		},
		client: client,
		q:      fakeQueue{},
	}
	ctx := context.Background()
	req := &regattapb.DeleteRangeRequest{
		Table: table1Name,
		Key:   key1Name,
	}
	client.On("DeleteRange", ctx, req, mock.Anything).Return(&regattapb.DeleteRangeResponse{Header: &regattapb.ResponseHeader{Revision: 1}}, nil)
	t.Log("Delete existing kv")
	resp, err := kv.DeleteRange(ctx, req)
	r.NoError(err)
	r.Equal(uint64(1), resp.Header.Revision)
}

func TestForwardingKVServer_Txn(t *testing.T) {
	r := require.New(t)
	storage := &mockKVService{}
	client := &mockClient{}
	kv := ForwardingKVServer{
		KVServer: KVServer{
			Storage: storage,
		},
		client: client,
		q:      fakeQueue{},
	}

	ctx := context.Background()
	req := &regattapb.TxnRequest{
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestPut{RequestPut: &regattapb.RequestOp_Put{
					Key: key1Name,
				}},
			},
		},
	}
	t.Log("Writable Txn")
	client.On("Txn", ctx, req, mock.Anything).Return(&regattapb.TxnResponse{Header: &regattapb.ResponseHeader{Revision: 1}}, nil)
	resp, err := kv.Txn(ctx, req)
	r.NoError(err)
	r.Equal(uint64(1), resp.Header.Revision)

	req = &regattapb.TxnRequest{
		Table: table1Name,
		Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestRange{RequestRange: &regattapb.RequestOp_Range{
					Key: key1Name,
				}},
			},
		},
	}
	ctx = context.Background()
	storage.On("Txn", ctx, req).Return(&regattapb.TxnResponse{}, nil)
	t.Log("Readonly Txn")
	_, err = kv.Txn(ctx, req)
	r.NoError(err)
}

// mockKVService implements trivial storage for testing purposes.
type mockKVService struct {
	mock.Mock
}

func (s *mockKVService) Range(ctx context.Context, req *regattapb.RangeRequest) (*regattapb.RangeResponse, error) {
	called := s.Mock.Called(ctx, req)
	return called.Get(0).(*regattapb.RangeResponse), called.Error(1)
}

func (s *mockKVService) IterateRange(ctx context.Context, req *regattapb.RangeRequest) (iter.Seq[*regattapb.RangeResponse], error) {
	called := s.Mock.Called(ctx, req)
	return called.Get(0).(iter.Seq[*regattapb.RangeResponse]), called.Error(1)
}

func (s *mockKVService) Put(ctx context.Context, req *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	called := s.Mock.Called(ctx, req)
	return called.Get(0).(*regattapb.PutResponse), called.Error(1)
}

func (s *mockKVService) Delete(ctx context.Context, req *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	called := s.Mock.Called(ctx, req)
	return called.Get(0).(*regattapb.DeleteRangeResponse), called.Error(1)
}

func (s *mockKVService) Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	called := s.Mock.Called(ctx, req)
	return called.Get(0).(*regattapb.TxnResponse), called.Error(1)
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

type mockClient struct {
	mock.Mock
}

func (m *mockClient) Range(ctx context.Context, in *regattapb.RangeRequest, opts ...grpc.CallOption) (*regattapb.RangeResponse, error) {
	called := m.Mock.Called(ctx, in, opts)
	return called.Get(0).(*regattapb.RangeResponse), called.Error(1)
}

func (m *mockClient) IterateRange(ctx context.Context, in *regattapb.RangeRequest, opts ...grpc.CallOption) (regattapb.KV_IterateRangeClient, error) {
	called := m.Mock.Called(ctx, in, opts)
	return called.Get(0).(regattapb.KV_IterateRangeClient), called.Error(1)
}

func (m *mockClient) Put(ctx context.Context, in *regattapb.PutRequest, opts ...grpc.CallOption) (*regattapb.PutResponse, error) {
	called := m.Mock.Called(ctx, in, opts)
	return called.Get(0).(*regattapb.PutResponse), called.Error(1)
}

func (m *mockClient) DeleteRange(ctx context.Context, in *regattapb.DeleteRangeRequest, opts ...grpc.CallOption) (*regattapb.DeleteRangeResponse, error) {
	called := m.Mock.Called(ctx, in, opts)
	return called.Get(0).(*regattapb.DeleteRangeResponse), called.Error(1)
}

func (m *mockClient) Txn(ctx context.Context, in *regattapb.TxnRequest, opts ...grpc.CallOption) (*regattapb.TxnResponse, error) {
	called := m.Mock.Called(ctx, in, opts)
	return called.Get(0).(*regattapb.TxnResponse), called.Error(1)
}

type fakeQueue struct{}

func (f fakeQueue) Add(ctx context.Context, table string, revision uint64) <-chan error {
	i := make(chan error)
	close(i)
	return i
}
