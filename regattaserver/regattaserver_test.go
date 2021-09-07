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
	rangeError          error
	putError            error
	deleteError         error
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

type MockTableService struct {
	tables []table.Table
	error  error
}

func (t MockTableService) GetTables() ([]table.Table, error) {
	return t.tables, t.error
}

func (t MockTableService) GetTable(name string) (table.ActiveTable, error) {
	return t.tables[0].AsActive(nil), t.error
}
