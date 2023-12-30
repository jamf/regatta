// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"io"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/util/iter"
)

// MockStorage implements trivial storage for testing purposes.
type MockStorage struct {
	rangeResponse        regattapb.RangeResponse
	iterateRangeResponse iter.Seq[*regattapb.RangeResponse]
	putResponse          regattapb.PutResponse
	deleteRangeResponse  regattapb.DeleteRangeResponse
	txnResponse          regattapb.TxnResponse
	rangeError           error
	putError             error
	deleteError          error
}

func (s *MockStorage) Range(_ context.Context, _ *regattapb.RangeRequest) (*regattapb.RangeResponse, error) {
	return &s.rangeResponse, s.rangeError
}

func (s *MockStorage) IterateRange(_ context.Context, _ *regattapb.RangeRequest) (iter.Seq[*regattapb.RangeResponse], error) {
	return s.iterateRangeResponse, s.rangeError
}

func (s *MockStorage) Put(_ context.Context, _ *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	return &s.putResponse, s.putError
}

func (s *MockStorage) Delete(_ context.Context, _ *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	return &s.deleteRangeResponse, s.deleteError
}

func (s *MockStorage) Txn(_ context.Context, _ *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	return &s.txnResponse, s.deleteError
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

func (t MockTableService) Restore(name string, reader io.Reader) error {
	return t.error
}
