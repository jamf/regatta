// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/proto"
)

type commandDelete struct {
	*proto.Command
}

func (c commandDelete) handle(ctx *updateContext) (UpdateResult, *proto.CommandResult, error) {
	resp, err := handleDelete(ctx, &proto.RequestOp_DeleteRange{
		Key:      c.Kv.Key,
		RangeEnd: c.RangeEnd,
		PrevKv:   c.PrevKvs,
		Count:    c.Count,
	})
	if err != nil {
		return ResultFailure, nil, err
	}
	return ResultSuccess, &proto.CommandResult{
		Revision:  ctx.index,
		Responses: []*proto.ResponseOp{wrapResponseOp(resp)},
	}, nil
}

func handleDelete(ctx *updateContext, del *proto.RequestOp_DeleteRange) (*proto.ResponseOp_DeleteRange, error) {
	resp := &proto.ResponseOp_DeleteRange{}
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
			rng, err := rangeLookup(ctx.batch, &proto.RequestOp_Range{Key: del.Key, RangeEnd: del.RangeEnd, CountOnly: del.Count && !del.PrevKv})
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
			rng, err := singleLookup(ctx.batch, &proto.RequestOp_Range{Key: del.Key, CountOnly: del.Count && !del.PrevKv})
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return nil, err
			}
			if !errors.Is(err, pebble.ErrNotFound) {
				resp.Deleted = 1
				resp.PrevKvs = rng.Kvs
			}
		}
		if err := ctx.batch.Delete(keyBuf.Bytes(), nil); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

type commandDeleteBatch struct {
	*proto.Command
}

func (c commandDeleteBatch) handle(ctx *updateContext) (UpdateResult, *proto.CommandResult, error) {
	req := make([]*proto.RequestOp_DeleteRange, len(c.Batch))
	for i, kv := range c.Batch {
		req[i] = &proto.RequestOp_DeleteRange{
			Key: kv.Key,
		}
	}
	rop, err := handleDeleteBatch(ctx, req)
	if err != nil {
		return ResultFailure, nil, err
	}
	res := make([]*proto.ResponseOp, 0, len(c.Batch))
	for _, put := range rop {
		res = append(res, wrapResponseOp(put))
	}
	return ResultSuccess, &proto.CommandResult{
		Revision:  ctx.index,
		Responses: res,
	}, nil
}

func handleDeleteBatch(ctx *updateContext, ops []*proto.RequestOp_DeleteRange) ([]*proto.ResponseOp_DeleteRange, error) {
	var results []*proto.ResponseOp_DeleteRange
	for _, op := range ops {
		res, err := handleDelete(ctx, op)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}
