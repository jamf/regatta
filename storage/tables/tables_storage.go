package tables

import (
	"context"
	"time"

	"github.com/wandera/regatta/proto"
)

type KVStorageWrapper struct {
	Manager *Manager
}

func (k *KVStorageWrapper) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return table.Range(dctx, req)
	}
	return table.Range(ctx, req)
}

func (k *KVStorageWrapper) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return table.Put(dctx, req)
	}
	return table.Put(ctx, req)
}

func (k *KVStorageWrapper) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return table.Delete(dctx, req)
	}
	return table.Delete(ctx, req)
}
