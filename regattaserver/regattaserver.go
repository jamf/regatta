// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"io"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/util/iter"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftpb"
)

type KVService interface {
	Range(ctx context.Context, req *regattapb.RangeRequest) (*regattapb.RangeResponse, error)
	Put(ctx context.Context, req *regattapb.PutRequest) (*regattapb.PutResponse, error)
	Delete(ctx context.Context, req *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error)
	Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error)
	IterateRange(ctx context.Context, req *regattapb.RangeRequest) (iter.Seq[*regattapb.RangeResponse], error)
}

type SnapshotService interface {
	Snapshot(ctx context.Context, writer io.Writer) error
}

type TableService interface {
	GetTables() ([]table.Table, error)
	GetTable(name string) (table.ActiveTable, error)
	Restore(name string, reader io.Reader) error
	CreateTable(name string) error
	DeleteTable(name string) error
}

type ClusterService interface {
	MemberList(context.Context, *regattapb.MemberListRequest) (*regattapb.MemberListResponse, error)
	Status(context.Context, *regattapb.StatusRequest) (*regattapb.StatusResponse, error)
}

type ConfigService func() map[string]any

type LogReaderService interface {
	QueryRaftLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error)
}
