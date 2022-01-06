package tables

import (
	"context"
	"time"

	"github.com/wandera/regatta/proto"
)

const defaultQueryTimeout = 5 * time.Second

type QueryService struct {
	Manager *Manager
}

func (k *QueryService) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ctx = dctx
	}
	return table.Range(ctx, req)
}

func (k *QueryService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	return table.Put(ctx, req)
}

func (k *QueryService) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	return table.Delete(ctx, req)
}

func (k *QueryService) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	table, err := k.Manager.GetTable(string(req.Table))
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		dctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
		ctx = dctx
	}
	return table.Txn(ctx, req)
}
