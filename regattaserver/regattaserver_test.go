// Copyright JAMF Software, LLC

package regattaserver

import (
	"io"

	"github.com/jamf/regatta/storage/table"
)

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
