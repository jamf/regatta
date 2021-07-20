package regattaserver

import (
	"context"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table"
)

const (
	managedTable = "managed-table"
)

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

func (s *MockStorage) Range(_ context.Context, _ *proto.RangeRequest) (*proto.RangeResponse, error) {
	return &s.rangeResponse, s.rangeError
}

func (s *MockStorage) Put(_ context.Context, _ *proto.PutRequest) (*proto.PutResponse, error) {
	return &s.putResponse, s.putError
}

func (s *MockStorage) Delete(_ context.Context, _ *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	return &s.deleteRangeResponse, s.deleteError
}

// Reset method resets storage.
func (s *MockStorage) Reset(_ context.Context, _ *proto.ResetRequest) (*proto.ResetResponse, error) {
	return &s.resetResponse, s.resetError
}

func (s *MockStorage) Hash(_ context.Context, _ *proto.HashRequest) (*proto.HashResponse, error) {
	return &s.hashResponse, s.hashError
}

type MockTableService struct {
	tables []table.Table
	error  error
}

func (t MockTableService) GetTables() ([]table.Table, error) {
	return t.tables, t.error
}
