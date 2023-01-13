// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"errors"

	"github.com/jamf/regatta/proto"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TableManager interface {
	GetTables() ([]table.Table, error)
	CreateTable(name string) error
	DeleteTable(name string) error
}

type FollowerTableServer struct {
	proto.UnimplementedFollowerTablesServer
	tm TableManager
}

func NewFollowerTableServer(tm TableManager) *FollowerTableServer {
	return &FollowerTableServer{tm: tm}
}

func (fts *FollowerTableServer) CreateTable(ctx context.Context, req *proto.CreateTableRequest) (*proto.CreateTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Service CreateTable not implemented")
}

func (fts *FollowerTableServer) DeleteTable(ctx context.Context, req *proto.DeleteTableRequest) (*proto.DeleteTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Service DeleteTable not implemented")
}

func (fts *FollowerTableServer) GetTables(ctx context.Context, req *proto.GetTableRequest) (*proto.FollowerGetTablesResponse, error) {
	tt, err := fts.tm.GetTables()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "unknown err %v", err)
	}

	res := &proto.FollowerGetTablesResponse{
		Tables: make([]*proto.FollowerTableInfo, len(tt)),
	}

	for i, t := range tt {
		res.Tables[i] = &proto.FollowerTableInfo{
			Type: proto.TableType_REPLICATED,
			Table: &proto.TableInfo{
				Table:   []byte(t.Name),
				ShardId: t.ClusterID,
			},
		}
	}

	return res, nil
}

type LeaderTableServer struct {
	proto.UnimplementedLeaderTablesServer
	tm TableManager
}

func NewLeaderTableServer(tm TableManager) *LeaderTableServer {
	return &LeaderTableServer{tm: tm}
}

func (lts LeaderTableServer) CreateTable(ctx context.Context, req *proto.CreateTableRequest) (*proto.CreateTableResponse, error) {
	err := lts.tm.CreateTable(string(req.Table))
	if errors.Is(err, serrors.ErrTableExists) {
		return nil, status.Errorf(codes.AlreadyExists, "table %s already exists", string(req.Table))
	} else if err != nil {
		return nil, status.Errorf(codes.Unknown, "unknown error: %v", err)
	}

	return &proto.CreateTableResponse{}, nil
}

func (lts LeaderTableServer) DeleteTable(ctx context.Context, req *proto.DeleteTableRequest) (*proto.DeleteTableResponse, error) {
	// TODO(jsfpdn): Fix lts.tm.DeleteTable returns key-value related errors instead of table related errors.
	return &proto.DeleteTableResponse{}, lts.tm.DeleteTable(string(req.Table))
}

func (lts LeaderTableServer) GetTables(ctx context.Context, req *proto.GetTableRequest) (*proto.GetTablesResponse, error) {
	tt, err := lts.tm.GetTables()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "unknown error %v", err)
	}

	res := &proto.GetTablesResponse{
		Tables: make([]*proto.TableInfo, len(tt)),
	}
	for i, t := range tt {
		res.Tables[i] = &proto.TableInfo{
			Table:   []byte(t.Name),
			ShardId: t.ClusterID,
		}
	}

	return res, nil
}
