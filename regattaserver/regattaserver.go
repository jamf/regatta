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
	Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error)
	Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error)
}

type SnapshotService interface {
	Snapshot(ctx context.Context, writer io.Writer) error
}

type TableService interface {
	GetTables() ([]table.Table, error)
	GetTable(name string) (table.ActiveTable, error)
}
