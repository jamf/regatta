// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"errors"

	"github.com/jamf/regatta/proto"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/storage/tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TableManager interface {
	GetTables() ([]table.Table, error)
	CreateTable(name string) error
	DeleteTable(name string) error
}

type GetTables func() ([]table.Table, error)

type FollowerTableServer struct {
	proto.UnimplementedTablesServer
	getTables GetTables
}

func NewFollowerTableServer(getTables GetTables) *FollowerTableServer {
	return &FollowerTableServer{getTables: getTables}
}

func (fts *FollowerTableServer) CreateTable(_ context.Context, _ *proto.CreateTableRequest) (*proto.CreateTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Service CreateTable not implemented in follower cluster. Call this procedure in the leader cluster.")
}

func (fts *FollowerTableServer) DeleteTable(_ context.Context, _ *proto.DeleteTableRequest) (*proto.DeleteTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Service DeleteTable not implemented in follower cluster. Call this procedure in the leader cluster.")
}

func (fts *FollowerTableServer) GetTables(_ context.Context, _ *proto.GetTableRequest) (*proto.GetTablesResponse, error) {
	return createResponse(fts.getTables)
}

type LeaderTableServer struct {
	proto.UnimplementedTablesServer
	tm TableManager
}

func NewLeaderTableServer(tm TableManager) *LeaderTableServer { return &LeaderTableServer{tm: tm} }

func (lts *LeaderTableServer) CreateTable(_ context.Context, req *proto.CreateTableRequest) (*proto.CreateTableResponse, error) {
	err := lts.tm.CreateTable(string(req.Table))
	if errors.Is(err, serrors.ErrTableExists) {
		return nil, status.Errorf(codes.AlreadyExists, "table %s already exists", string(req.Table))
	} else if err != nil {
		return nil, status.Errorf(codes.Unknown, "unknown error: %v", err)
	}

	return &proto.CreateTableResponse{}, nil
}

func (lts *LeaderTableServer) DeleteTable(_ context.Context, req *proto.DeleteTableRequest) (*proto.DeleteTableResponse, error) {
	err := lts.tm.DeleteTable(string(req.Table))
	if errors.Is(err, tables.ErrTableNotExists) {
		return nil, status.Errorf(codes.NotFound, "table %s not found", string(req.Table))
	} else if err != nil {
		return nil, status.Errorf(codes.Unknown, "unknown error: %v", err)
	}

	return &proto.DeleteTableResponse{}, nil
}

func (lts *LeaderTableServer) GetTables(_ context.Context, _ *proto.GetTableRequest) (*proto.GetTablesResponse, error) {
	return createResponse(lts.tm.GetTables)
}

// createResponse for the GetTables endpoint.
func createResponse(getTables func() ([]table.Table, error)) (*proto.GetTablesResponse, error) {
	tt, err := getTables()
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
