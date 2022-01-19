package fsm

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

// Update updates the object.
func (p *FSM) Update(updates []sm.Entry) ([]sm.Entry, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))

	ctx := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		wo:    p.wo,
		cmd:   proto.CommandFromVTPool(),
	}

	defer func() {
		_ = ctx.Close()
	}()

	for i := 0; i < len(updates); i++ {
		if err := ctx.Init(updates[i]); err != nil {
			return nil, err
		}

		res := &proto.CommandResult{}
		switch ctx.cmd.Type {
		case proto.Command_PUT:
			rop, err := handlePut(ctx, &proto.RequestOp_Put{
				Key:    ctx.cmd.Kv.Key,
				Value:  ctx.cmd.Kv.Value,
				PrevKv: false,
			})
			if err != nil {
				return nil, err
			}
			res.Responses = append(res.Responses, wrapResponseOp(rop))
		case proto.Command_DELETE:
			rop, err := handleDelete(ctx, &proto.RequestOp_DeleteRange{
				Key:      ctx.cmd.Kv.Key,
				RangeEnd: ctx.cmd.RangeEnd,
				PrevKv:   false,
			})
			if err != nil {
				return nil, err
			}
			res.Responses = append(res.Responses, wrapResponseOp(rop))
		case proto.Command_PUT_BATCH:
			req := make([]*proto.RequestOp_Put, len(ctx.cmd.Batch))
			for i, kv := range ctx.cmd.Batch {
				req[i] = &proto.RequestOp_Put{
					Key:   kv.Key,
					Value: kv.Value,
				}
			}
			rop, err := handlePutBatch(ctx, req)
			if err != nil {
				return nil, err
			}
			for _, put := range rop {
				res.Responses = append(res.Responses, wrapResponseOp(put))
			}
		case proto.Command_DELETE_BATCH:
			req := make([]*proto.RequestOp_DeleteRange, len(ctx.cmd.Batch))
			for i, kv := range ctx.cmd.Batch {
				req[i] = &proto.RequestOp_DeleteRange{
					Key: kv.Key,
				}
			}
			rop, err := handleDeleteBatch(ctx, req)
			if err != nil {
				return nil, err
			}
			for _, del := range rop {
				res.Responses = append(res.Responses, wrapResponseOp(del))
			}
		case proto.Command_TXN:
			rop, err := handleTxn(ctx)
			if err != nil {
				return nil, err
			}
			res.Responses = append(res.Responses, rop...)
		case proto.Command_DUMMY:
		}

		updates[i].Result.Value = ResultSuccess
		if len(res.Responses) > 0 {
			bts, err := res.MarshalVT()
			if err != nil {
				return nil, err
			}
			updates[i].Result.Data = bts
		}
	}

	if err := ctx.Commit(); err != nil {
		return nil, err
	}

	return updates, nil
}

func handlePut(ctx *updateContext, put *proto.RequestOp_Put) (*proto.ResponseOp_Put, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	if err := encodeUserKey(keyBuf, put.Key); err != nil {
		return nil, err
	}
	if err := ctx.batch.Set(keyBuf.Bytes(), put.Value, nil); err != nil {
		return nil, err
	}
	return &proto.ResponseOp_Put{}, nil
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

func handleDelete(ctx *updateContext, del *proto.RequestOp_DeleteRange) (*proto.ResponseOp_DeleteRange, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	if err := encodeUserKey(keyBuf, del.Key); err != nil {
		return nil, err
	}

	if del.RangeEnd != nil {
		var end []byte
		if bytes.Equal(del.RangeEnd, wildcard) {
			// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum key.
			end = incrementRightmostByte(maxUserKey)
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
		if err := ctx.batch.Delete(keyBuf.Bytes(), nil); err != nil {
			return nil, err
		}
	}
	return &proto.ResponseOp_DeleteRange{}, nil
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

func handleTxn(ctx *updateContext) ([]*proto.ResponseOp, error) {
	if err := ctx.EnsureIndexed(); err != nil {
		return nil, err
	}
	ok, err := txnCompare(ctx.batch, ctx.cmd.Txn.Compare)
	if err != nil {
		return nil, err
	}
	if ok {
		return handleTxnOps(ctx, ctx.cmd.Txn.Success)
	}
	return handleTxnOps(ctx, ctx.cmd.Txn.Failure)
}

func handleTxnOps(ctx *updateContext, req []*proto.RequestOp) ([]*proto.ResponseOp, error) {
	var results []*proto.ResponseOp
	for _, op := range req {
		switch o := op.Request.(type) {
		case *proto.RequestOp_RequestRange:
			var (
				err      error
				response *proto.ResponseOp_Range
			)

			if o.RequestRange.RangeEnd != nil {
				response, err = rangeLookup(ctx.batch, o)
			} else {
				response, err = singleLookup(ctx.batch, o)
			}
			if err != nil {
				return nil, err
			}
			results = append(results, &proto.ResponseOp{Response: &proto.ResponseOp_ResponseRange{ResponseRange: response}})
		case *proto.RequestOp_RequestPut:
			response, err := handlePut(ctx, o.RequestPut)
			if err != nil {
				return nil, err
			}
			results = append(results, wrapResponseOp(response))
		case *proto.RequestOp_RequestDeleteRange:
			response, err := handleDelete(ctx, o.RequestDeleteRange)
			if err != nil {
				return nil, err
			}

			results = append(results, wrapResponseOp(response))
		}
	}
	return results, nil
}

func wrapRequestOp(req pb.Message) *proto.RequestOp {
	switch op := req.(type) {
	case *proto.RequestOp_Range:
		return &proto.RequestOp{Request: &proto.RequestOp_RequestRange{RequestRange: op}}
	case *proto.RequestOp_Put:
		return &proto.RequestOp{Request: &proto.RequestOp_RequestPut{RequestPut: op}}
	case *proto.RequestOp_DeleteRange:
		return &proto.RequestOp{Request: &proto.RequestOp_RequestDeleteRange{RequestDeleteRange: op}}
	}
	return nil
}

func wrapResponseOp(req pb.Message) *proto.ResponseOp {
	switch op := req.(type) {
	case *proto.ResponseOp_Range:
		return &proto.ResponseOp{Response: &proto.ResponseOp_ResponseRange{ResponseRange: op}}
	case *proto.ResponseOp_Put:
		return &proto.ResponseOp{Response: &proto.ResponseOp_ResponsePut{ResponsePut: op}}
	case *proto.ResponseOp_DeleteRange:
		return &proto.ResponseOp{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: op}}
	}
	return nil
}

type updateContext struct {
	batch *pebble.Batch
	wo    *pebble.WriteOptions
	db    *pebble.DB
	cmd   *proto.Command
	index uint64
}

func (c *updateContext) EnsureIndexed() error {
	if c.batch.Indexed() {
		return nil
	}

	indexed := c.db.NewIndexedBatch()
	if err := indexed.Apply(c.batch, nil); err != nil {
		return err
	}
	if err := c.batch.Close(); err != nil {
		return err
	}
	c.batch = indexed
	return nil
}

func (c *updateContext) Init(entry sm.Entry) error {
	c.index = entry.Index
	c.cmd.ResetVT()
	if err := c.cmd.UnmarshalVT(entry.Cmd); err != nil {
		return err
	}
	return nil
}

func (c *updateContext) Commit() error {
	// Set leader index if present in the proposal
	if c.cmd.LeaderIndex != nil {
		leaderIdx := make([]byte, 8)
		binary.LittleEndian.PutUint64(leaderIdx, *c.cmd.LeaderIndex)
		if err := c.batch.Set(sysLeaderIndex, leaderIdx, nil); err != nil {
			return err
		}
	}
	// Set local index
	idx := make([]byte, 8)
	binary.LittleEndian.PutUint64(idx, c.index)
	if err := c.batch.Set(sysLocalIndex, idx, nil); err != nil {
		return err
	}
	return c.batch.Commit(c.wo)
}

func (c *updateContext) Close() error {
	if err := c.batch.Close(); err != nil {
		return err
	}
	c.cmd.ReturnToVTPool()
	return nil
}
