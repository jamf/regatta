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
			result = sm.Result{Value: ResultSuccess}
			err    error
		)

		switch ctx.cmd.Type {
		case proto.Command_PUT:
			if _, err = handlePut(ctx); err != nil {
				result = sm.Result{Value: ResultFailure}
			}
		case proto.Command_DELETE:
			if _, err = handleDelete(ctx); err != nil {
				result = sm.Result{Value: ResultFailure}
			}
		case proto.Command_PUT_BATCH:
			if _, err = handlePutBatch(ctx); err != nil {
				result = sm.Result{Value: ResultFailure}
			}
		case proto.Command_DELETE_BATCH:
			if _, err = handleDeleteBatch(ctx); err != nil {
				result = sm.Result{Value: ResultFailure}
			}
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

func handlePut(ctx *updateContext) (*proto.ResponseOp_Put, error) {
	if err := encodeUserKey(ctx.keyBuf, ctx.cmd.Kv.Key); err != nil {
		return nil, err
	}
	if err := ctx.batch.Set(ctx.keyBuf.Bytes(), ctx.cmd.Kv.Value, nil); err != nil {
		return nil, err
	}
	return &proto.ResponseOp_Put{}, nil
}

func handlePutBatch(ctx *updateContext) (*proto.ResponseOp_Put, error) {
	for _, kv := range ctx.cmd.Batch {
		ctx.cmd.Kv = kv
		if _, err := handlePut(ctx); err != nil {
			return nil, err
		}
		ctx.keyBuf.Reset()
	}
	return &proto.ResponseOp_Put{}, nil
}

func handleDelete(ctx *updateContext) (*proto.ResponseOp_DeleteRange, error) {
	if err := encodeUserKey(ctx.keyBuf, ctx.cmd.Kv.Key); err != nil {
		return nil, err
	}

	if ctx.cmd.RangeEnd != nil {
		var end []byte
		if bytes.Equal(ctx.cmd.RangeEnd, wildcard) {
			// In order to include the last key in the iterator as well we have to increment the rightmost byte of the maximum key.
			end = incrementRightmostByte(maxUserKey)
		} else {
			upperBoundBuf := bytes.NewBuffer(make([]byte, 0, key.LatestKeyLen(len(ctx.cmd.RangeEnd))))
			if err := encodeUserKey(upperBoundBuf, ctx.cmd.RangeEnd); err != nil {
				return nil, err
			}
			end = upperBoundBuf.Bytes()
		}

		if err := ctx.batch.DeleteRange(ctx.keyBuf.Bytes(), end, nil); err != nil {
			return nil, err
		}
	} else {
		if err := ctx.batch.Delete(ctx.keyBuf.Bytes(), nil); err != nil {
			return nil, err
		}
	}
	return &proto.ResponseOp_DeleteRange{}, nil
}

func handleDeleteBatch(ctx *updateContext) (*proto.ResponseOp_DeleteRange, error) {
	for _, kv := range ctx.cmd.Batch {
		ctx.cmd.Kv = kv
		if _, err := handleDelete(ctx); err != nil {
			return nil, err
		}
		ctx.keyBuf.Reset()
	}
	return &proto.ResponseOp_DeleteRange{}, nil
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
			var (
				err      error
				response *proto.ResponseOp_Range
			)

			if o.RequestRange.RangeEnd != nil {
				response, err = rangeLookup(ctx.db, o)
			} else {
				response, err = singleLookup(ctx.db, o, ctx.keyBuf)
			}

			if err != nil {
				return sm.Result{Value: ResultFailure}, err
			}

			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponseRange{ResponseRange: response}})
		case *proto.RequestOp_RequestPut:
			ctx.cmd.Kv.Key = o.RequestPut.Key
			ctx.cmd.Kv.Value = o.RequestPut.Value

			response, err := handlePut(ctx)
			if err != nil {
				return sm.Result{Value: ResultFailure}, err
			}
			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponsePut{ResponsePut: response}})
		case *proto.RequestOp_RequestDeleteRange:
			ctx.cmd.RangeEnd = o.RequestDeleteRange.RangeEnd
			ctx.cmd.Kv.Key = o.RequestDeleteRange.Key

			response, err := handleDelete(ctx)
			if err != nil {
				return sm.Result{Value: ResultFailure}, err
			}

			res.Responses = append(res.Responses, &proto.ResponseOp{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: response}})
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
