// Copyright JAMF Software, LLC

package fsm

import (
	"github.com/jamf/regatta/regattapb"
)

type commandPut struct {
	*regattapb.Command
}

func (c commandPut) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	resp, err := handlePut(ctx, &regattapb.RequestOp_Put{
		Key:    c.Kv.Key,
		Value:  c.Kv.Value,
		PrevKv: c.PrevKvs,
	})
	if err != nil {
		return ResultFailure, nil, err
	}
	return ResultSuccess, &regattapb.CommandResult{
		Revision:  ctx.index,
		Responses: []*regattapb.ResponseOp{wrapResponseOp(resp)},
	}, nil
}

func handlePut(ctx *updateContext, put *regattapb.RequestOp_Put) (*regattapb.ResponseOp_Put, error) {
	resp := &regattapb.ResponseOp_Put{}
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	if err := encodeUserKey(keyBuf, put.Key); err != nil {
		return nil, err
	}
	if put.PrevKv {
		if err := ctx.EnsureIndexed(); err != nil {
			return nil, err
		}
		rng, err := singleLookup(ctx.batch, &regattapb.RequestOp_Range{Key: put.Key})
		if err != nil {
			return nil, err
		}
		if len(rng.Kvs) == 1 {
			resp.PrevKv = rng.Kvs[0]
		}
	}
	if err := ctx.batch.Set(keyBuf.Bytes(), put.Value, nil); err != nil {
		return nil, err
	}
	return resp, nil
}

type commandPutBatch struct {
	*regattapb.Command
}

func (c commandPutBatch) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	req := make([]*regattapb.RequestOp_Put, len(c.Batch))
	for i, kv := range c.Batch {
		req[i] = &regattapb.RequestOp_Put{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}
	rop, err := handlePutBatch(ctx, req)
	if err != nil {
		return ResultFailure, nil, err
	}
	res := make([]*regattapb.ResponseOp, 0, len(c.Batch))
	for _, put := range rop {
		res = append(res, wrapResponseOp(put))
	}
	return ResultSuccess, &regattapb.CommandResult{
		Revision:  ctx.index,
		Responses: res,
	}, nil
}

func handlePutBatch(ctx *updateContext, ops []*regattapb.RequestOp_Put) ([]*regattapb.ResponseOp_Put, error) {
	var results []*regattapb.ResponseOp_Put
	for _, op := range ops {
		res, err := handlePut(ctx, op)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}
