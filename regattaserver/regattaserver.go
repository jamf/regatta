package regattaserver

import (
	"context"
	"io"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table"
)

type KVService interface {
	Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error)
	Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error)
	Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error)
	Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error)
}

type SnapshotService interface {
	Snapshot(ctx context.Context, writer io.Writer) error
}

type TableService interface {
	GetTables() ([]table.Table, error)
	GetTable(name string) (table.ActiveTable, error)
	Restore(name string, reader io.Reader) error
}
