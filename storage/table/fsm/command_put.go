package fsm

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/wandera/regatta/proto"
)

type commandPut struct {
	*updateContext
	command *proto.Command
}

func (c commandPut) handle() (UpdateResult, *proto.CommandResult, error) {
	resp, err := handlePut(c.updateContext, &proto.RequestOp_Put{
		Key:    c.command.Kv.Key,
		Value:  c.command.Kv.Value,
		PrevKv: c.command.PrevKvs,
	})
	if err != nil {
		return ResultFailure, nil, err
	}
	return ResultSuccess, &proto.CommandResult{
		Revision:  c.index,
		Responses: []*proto.ResponseOp{wrapResponseOp(resp)},
	}, nil
}

func handlePut(ctx *updateContext, put *proto.RequestOp_Put) (*proto.ResponseOp_Put, error) {
	resp := &proto.ResponseOp_Put{}
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	if err := encodeUserKey(keyBuf, put.Key); err != nil {
		return nil, err
	}
	if put.PrevKv {
		if err := ctx.EnsureIndexed(); err != nil {
			return nil, err
		}
		rng, err := singleLookup(ctx.batch, &proto.RequestOp_Range{Key: put.Key})
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			resp.PrevKv = rng.Kvs[0]
		}
	}
	if err := ctx.batch.Set(keyBuf.Bytes(), put.Value, nil); err != nil {
		return nil, err
	}
	return resp, nil
}

type commandPutBatch struct {
	*updateContext
	command *proto.Command
}

func (c commandPutBatch) handle() (UpdateResult, *proto.CommandResult, error) {
	req := make([]*proto.RequestOp_Put, len(c.command.Batch))
	for i, kv := range c.command.Batch {
		req[i] = &proto.RequestOp_Put{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}
	rop, err := handlePutBatch(c.updateContext, req)
	if err != nil {
		return ResultFailure, nil, err
	}
	res := make([]*proto.ResponseOp, 0, len(c.command.Batch))
	for _, put := range rop {
		res = append(res, wrapResponseOp(put))
	}
	return ResultSuccess, &proto.CommandResult{
		Revision:  c.index,
		Responses: res,
	}, nil
}

func handlePutBatch(ctx *updateContext, ops []*proto.RequestOp_Put) ([]*proto.ResponseOp_Put, error) {
	var results []*proto.ResponseOp_Put
	for _, op := range ops {
		res, err := handlePut(ctx, op)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}
