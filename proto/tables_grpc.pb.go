// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TablesClient is the client API for Tables service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TablesClient interface {
	// Create a table. All followers will automatically replicate the table.
	// This procedure is available only in the leader cluster.
	CreateTable(ctx context.Context, in *CreateTableRequest, opts ...grpc.CallOption) (*CreateTableResponse, error)
	// Delete a table. All followers will automatically delete the table.
	// This procedure is available only in the leader cluster.
	DeleteTable(ctx context.Context, in *DeleteTableRequest, opts ...grpc.CallOption) (*DeleteTableResponse, error)
	// Get names of all the tables present in the cluster.
	// This procedure is available in both leader and follower clusters.
	GetTables(ctx context.Context, in *GetTableRequest, opts ...grpc.CallOption) (*GetTablesResponse, error)
}

type tablesClient struct {
	cc grpc.ClientConnInterface
}

func NewTablesClient(cc grpc.ClientConnInterface) TablesClient {
	return &tablesClient{cc}
}

func (c *tablesClient) CreateTable(ctx context.Context, in *CreateTableRequest, opts ...grpc.CallOption) (*CreateTableResponse, error) {
	out := new(CreateTableResponse)
	err := c.cc.Invoke(ctx, "/tables.v1.Tables/CreateTable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tablesClient) DeleteTable(ctx context.Context, in *DeleteTableRequest, opts ...grpc.CallOption) (*DeleteTableResponse, error) {
	out := new(DeleteTableResponse)
	err := c.cc.Invoke(ctx, "/tables.v1.Tables/DeleteTable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tablesClient) GetTables(ctx context.Context, in *GetTableRequest, opts ...grpc.CallOption) (*GetTablesResponse, error) {
	out := new(GetTablesResponse)
	err := c.cc.Invoke(ctx, "/tables.v1.Tables/GetTables", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TablesServer is the server API for Tables service.
// All implementations must embed UnimplementedTablesServer
// for forward compatibility
type TablesServer interface {
	// Create a table. All followers will automatically replicate the table.
	// This procedure is available only in the leader cluster.
	CreateTable(context.Context, *CreateTableRequest) (*CreateTableResponse, error)
	// Delete a table. All followers will automatically delete the table.
	// This procedure is available only in the leader cluster.
	DeleteTable(context.Context, *DeleteTableRequest) (*DeleteTableResponse, error)
	// Get names of all the tables present in the cluster.
	// This procedure is available in both leader and follower clusters.
	GetTables(context.Context, *GetTableRequest) (*GetTablesResponse, error)
	mustEmbedUnimplementedTablesServer()
}

// UnimplementedTablesServer must be embedded to have forward compatible implementations.
type UnimplementedTablesServer struct {
}

func (UnimplementedTablesServer) CreateTable(context.Context, *CreateTableRequest) (*CreateTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTable not implemented")
}
func (UnimplementedTablesServer) DeleteTable(context.Context, *DeleteTableRequest) (*DeleteTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteTable not implemented")
}
func (UnimplementedTablesServer) GetTables(context.Context, *GetTableRequest) (*GetTablesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTables not implemented")
}
func (UnimplementedTablesServer) mustEmbedUnimplementedTablesServer() {}

// UnsafeTablesServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TablesServer will
// result in compilation errors.
type UnsafeTablesServer interface {
	mustEmbedUnimplementedTablesServer()
}

func RegisterTablesServer(s grpc.ServiceRegistrar, srv TablesServer) {
	s.RegisterService(&Tables_ServiceDesc, srv)
}

func _Tables_CreateTable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TablesServer).CreateTable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tables.v1.Tables/CreateTable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TablesServer).CreateTable(ctx, req.(*CreateTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Tables_DeleteTable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TablesServer).DeleteTable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tables.v1.Tables/DeleteTable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TablesServer).DeleteTable(ctx, req.(*DeleteTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Tables_GetTables_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TablesServer).GetTables(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tables.v1.Tables/GetTables",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TablesServer).GetTables(ctx, req.(*GetTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Tables_ServiceDesc is the grpc.ServiceDesc for Tables service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Tables_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "tables.v1.Tables",
	HandlerType: (*TablesServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateTable",
			Handler:    _Tables_CreateTable_Handler,
		},
		{
			MethodName: "DeleteTable",
			Handler:    _Tables_DeleteTable_Handler,
		},
		{
			MethodName: "GetTables",
			Handler:    _Tables_GetTables_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "tables.proto",
}
