// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"

	"github.com/jamf/regatta/regattapb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClusterServer struct {
	regattapb.UnimplementedClusterServer
	Cluster ClusterService
}

func (c *ClusterServer) MemberList(ctx context.Context, req *regattapb.MemberListRequest) (*regattapb.MemberListResponse, error) {
	res, err := c.Cluster.MemberList(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return res, nil
}

func (c *ClusterServer) Status(ctx context.Context, req *regattapb.StatusRequest) (*regattapb.StatusResponse, error) {
	res, err := c.Cluster.Status(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return res, nil
}
