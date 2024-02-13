// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"

	"github.com/jamf/regatta/regattapb"
)

type commandDelete struct {
	*regattapb.Command
}

func (c commandDelete) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	resp, err := handleDelete(ctx, &regattapb.RequestOp_DeleteRange{
		Key:      c.Kv.Key,
		RangeEnd: c.RangeEnd,
		PrevKv:   c.PrevKvs,
		Count:    c.Count,
	})
	if err != nil {
		return ResultFailure, nil, err
	}
	return ResultSuccess, &regattapb.CommandResult{
		Revision:  ctx.index,
		Responses: []*regattapb.ResponseOp{wrapResponseOp(resp)},
	}, nil
}

func handleDelete(ctx *updateContext, del *regattapb.RequestOp_DeleteRange) (*regattapb.ResponseOp_DeleteRange, error) {
	resp := &regattapb.ResponseOp_DeleteRange{}
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	if err := encodeUserKey(keyBuf, del.Key); err != nil {
		return nil, err
	}

	if del.RangeEnd != nil {
		if del.PrevKv || del.Count {
			if err := ctx.EnsureIndexed(); err != nil {
				return nil, err
			}
			rng, err := rangeLookup(ctx.batch, &regattapb.RequestOp_Range{Key: del.Key, RangeEnd: del.RangeEnd, CountOnly: del.Count && !del.PrevKv})
			if err != nil {
				return nil, err
			}
			resp.Deleted = rng.Count
			resp.PrevKvs = rng.Kvs
		}

		var end []byte
		if bytes.Equal(del.RangeEnd, wildcard) {
			// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum key.
			end = make([]byte, len(maxUserKey))
			copy(end, maxUserKey)
			end = incrementRightmostByte(end)
		} else {
			upperBoundBuf := bufferPool.Get()
			defer bufferPool.Put(upperBoundBuf)

			if err := encodeUserKey(upperBoundBuf, del.RangeEnd); err != nil {
				return nil, err
			}
			end = upperBoundBuf.Bytes()
		}

		if err := ctx.batch.DeleteRange(keyBuf.Bytes(), end, nil); err != nil {
			return nil, err
		}
	} else {
		if del.PrevKv || del.Count {
			if err := ctx.EnsureIndexed(); err != nil {
				return nil, err
			}
			rng, err := singleLookup(ctx.batch, &regattapb.RequestOp_Range{Key: del.Key, CountOnly: del.Count && !del.PrevKv})
			if err != nil {
				return nil, err
			}
			resp.Deleted = rng.Count
			resp.PrevKvs = rng.Kvs
		}
		if err := ctx.batch.Delete(keyBuf.Bytes(), nil); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

type commandDeleteBatch struct {
	*regattapb.Command
}

func (c commandDeleteBatch) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	req := make([]*regattapb.RequestOp_DeleteRange, len(c.Batch))
	for i, kv := range c.Batch {
		req[i] = &regattapb.RequestOp_DeleteRange{
			Key: kv.Key,
		}
	}
	rop, err := handleDeleteBatch(ctx, req)
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

func handleDeleteBatch(ctx *updateContext, ops []*regattapb.RequestOp_DeleteRange) ([]*regattapb.ResponseOp_DeleteRange, error) {
	results := make([]*regattapb.ResponseOp_DeleteRange, len(ops))
	for i, op := range ops {
		res, err := handleDelete(ctx, op)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	return results, nil
}
