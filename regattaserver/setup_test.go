package regattaserver

import (
	"context"
	"os"
	"testing"

	"github.com/wandera/regatta/proto"
)

const (
	managedTable  = "managed-table"
	managedTable2 = "managed-table-2"
)

func setup() {
	s := MockStorage{}
	_, _ = s.Reset(context.TODO(), &proto.ResetRequest{})

	kv = KVServer{
		Storage:       &s,
		ManagedTables: []string{managedTable, managedTable2},
	}

	ms = MaintenanceServer{
		Storage: &s,
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

// MockStorage implements trivial storage for testing purposes.
type MockStorage struct {
	rangeResponse       proto.RangeResponse
	putResponse         proto.PutResponse
	deleteRangeResponse proto.DeleteRangeResponse
	resetResponse       proto.ResetResponse
	hashResponse        proto.HashResponse
	rangeError          error
	putError            error
	deleteError         error
	resetError          error
	hashError           error
}

func (s *MockStorage) Range(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	return &s.rangeResponse, s.rangeError
}

func (s *MockStorage) Put(_ context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	return &s.putResponse, s.putError
}

func (s *MockStorage) Delete(_ context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	return &s.deleteRangeResponse, s.deleteError
}

// Reset method resets storage.
func (s *MockStorage) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	return &s.resetResponse, s.resetError
}

func (s *MockStorage) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	return &s.hashResponse, s.hashError
}
