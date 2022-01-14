package fsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

// Update updates the object.
func (p *FSM) Update(updates []sm.Entry) ([]sm.Entry, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))

	ctx := &updateContext{
		batch:  db.NewBatch(),
		db:     db,
		wo:     p.wo,
		cmd:    proto.CommandFromVTPool(),
		keyBuf: bytes.NewBuffer(make([]byte, 0, key.LatestVersionLen)),
	}

	defer func() {
		_ = ctx.Close()
	}()

	for i := 0; i < len(updates); i++ {
		if err := ctx.Init(updates[i]); err != nil {
			return nil, err
		}

		var (
			result sm.Result
			err    error
		)
		switch ctx.cmd.Type {
		case proto.Command_PUT:
			result, err = handlePut(ctx)
		case proto.Command_DELETE:
			result, err = handleDelete(ctx)
		case proto.Command_PUT_BATCH:
			result, err = handlePutBatch(ctx)
		case proto.Command_DELETE_BATCH:
			result, err = handleDeleteBatch(ctx)
		case proto.Command_TXN:
			result, err = handleTxn(ctx)
		case proto.Command_DUMMY:
			result = sm.Result{Value: ResultSuccess}
		}
		if err != nil {
			return nil, err
		}
		updates[i].Result = result
	}

	if err := ctx.Commit(); err != nil {
		return nil, err
	}

	return updates, nil
}

func handlePut(ctx *updateContext) (sm.Result, error) {
	if err := encodeUserKey(ctx.keyBuf, ctx.cmd.Kv.Key); err != nil {
		return sm.Result{Value: ResultFailure}, err
	}
	if err := ctx.batch.Set(ctx.keyBuf.Bytes(), ctx.cmd.Kv.Value, nil); err != nil {
		return sm.Result{Value: ResultFailure}, err
	}
	return sm.Result{Value: ResultSuccess}, nil
}

func handleDelete(ctx *updateContext) (sm.Result, error) {
	return delete(ctx.cmd.Kv.Key, ctx.cmd.RangeEnd, ctx.keyBuf, ctx.batch)
}

func handlePutBatch(ctx *updateContext) (sm.Result, error) {
	for _, kv := range ctx.cmd.Batch {
		if err := encodeUserKey(ctx.keyBuf, kv.Key); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
		if err := ctx.batch.Set(ctx.keyBuf.Bytes(), kv.Value, nil); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
		ctx.keyBuf.Reset()
	}
	return sm.Result{Value: ResultSuccess}, nil
}

func handleDeleteBatch(ctx *updateContext) (sm.Result, error) {
	for _, kv := range ctx.cmd.Batch {
		if err := encodeUserKey(ctx.keyBuf, kv.Key); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
		if err := ctx.batch.Delete(ctx.keyBuf.Bytes(), nil); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
		ctx.keyBuf.Reset()
	}
	return sm.Result{Value: ResultSuccess}, nil
}

func handleTxn(ctx *updateContext) (sm.Result, error) {
	if err := ctx.EnsureIndexed(); err != nil {
		return sm.Result{Value: ResultFailure}, err
	}

	ok, err := handleTxnCompare(ctx, ctx.cmd.Txn.Compare)
	if err != nil {
		return sm.Result{}, err
	}

	if ok {
		return handleTxnOps(ctx, ctx.cmd.Txn.Success)
	}
	return handleTxnOps(ctx, ctx.cmd.Txn.Failure)
}

func handleTxnCompare(ctx *updateContext, compare []*proto.Compare) (bool, error) {
	result := true
	for _, cmp := range compare {
		if err := encodeUserKey(ctx.keyBuf, cmp.Key); err != nil {
			return false, err
		}
		_, closer, err := ctx.batch.Get(ctx.keyBuf.Bytes())
		// TODO other checks
		result = result && !errors.Is(err, pebble.ErrNotFound)
		if closer != nil {
			if err := closer.Close(); err != nil {
				return false, err
			}
		}
		ctx.keyBuf.Reset()
	}
	return result, nil
}

func handleTxnOps(ctx *updateContext, req []*proto.RequestOp) (sm.Result, error) {
	res := &proto.CommandResult{}
	for _, op := range req {
		switch o := op.Request.(type) {
		case *proto.RequestOp_RequestRange:
			req := &proto.RangeRequest{
				Key:       o.RequestRange.Key,
				KeysOnly:  o.RequestRange.KeysOnly,
				CountOnly: o.RequestRange.CountOnly,
			}

			var (
				err      error
				response *proto.RangeResponse
			)

			if o.RequestRange.RangeEnd != nil {
				req.RangeEnd = o.RequestRange.RangeEnd
				response, err = rangeLookup(ctx.db, req)
			} else {
				response, err = singleLookup(ctx.db, req, ctx.keyBuf)
			}

			if err != nil {
				return sm.Result{Value: ResultFailure}, err
			}

			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponseRange{ResponseRange: &proto.ResponseOp_Range{
				Kvs:   response.Kvs,
				More:  response.More,
				Count: response.Count,
			}}})
		case *proto.RequestOp_RequestPut:
			if err := encodeUserKey(ctx.keyBuf, o.RequestPut.Key); err != nil {
				return sm.Result{Value: ResultFailure}, err
			}
			if err := ctx.batch.Set(ctx.keyBuf.Bytes(), o.RequestPut.Value, nil); err != nil {
				return sm.Result{Value: ResultFailure}, err
			}
			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}})
		case *proto.RequestOp_RequestDeleteRange:
			// TODO(jsfpdn): If there are many DeleteRange requests, it would be beneficiary to also reuse the
			//				 the buffer used for the `upperBound`, just as for `lowerBound`.
			if smRes, err := delete(o.RequestDeleteRange.Key, o.RequestDeleteRange.RangeEnd, ctx.keyBuf, ctx.batch); err != nil {
				return smRes, err
			}
			// TODO(jsfpdn): Does `ctx.batch.Count()` say how many records were deleted with `RangeDelete`?
			// 				 Add the number of deleted records to the response.
			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}}})
		}
		ctx.keyBuf.Reset()
	}
	bts, err := res.MarshalVT()
	if err != nil {
		return sm.Result{Value: ResultFailure}, err
	}
	return sm.Result{Value: ResultSuccess, Data: bts}, nil
}

type updateContext struct {
	batch  *pebble.Batch
	wo     *pebble.WriteOptions
	db     *pebble.DB
	cmd    *proto.Command
	keyBuf *bytes.Buffer
	index  uint64
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
	c.keyBuf.Reset()
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

func delete(lowerBound, upperBound []byte, lowerBoundBuf *bytes.Buffer, batch *pebble.Batch) (sm.Result, error) {
	if err := encodeUserKey(lowerBoundBuf, lowerBound); err != nil {
		return sm.Result{Value: ResultFailure}, err
	}

	if upperBound != nil {
		var end []byte
		if bytes.Equal(upperBound, wildcard) {
			// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum key.
			end = incrementRightmostByte(maxUserKey)
		} else {
			upperBoundBuf := bytes.NewBuffer(make([]byte, 0, key.LatestKeyLen(len(upperBound))))
			if err := encodeUserKey(upperBoundBuf, upperBound); err != nil {
				return sm.Result{Value: ResultFailure}, err
			}
			end = upperBoundBuf.Bytes()
		}

		if err := batch.DeleteRange(lowerBoundBuf.Bytes(), end, nil); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
	} else {
		if err := batch.Delete(lowerBoundBuf.Bytes(), nil); err != nil {
			return sm.Result{Value: ResultFailure}, err
		}
	}
	return sm.Result{Value: ResultSuccess}, nil
}
