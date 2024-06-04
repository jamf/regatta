// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/regattapb"
)

type commandTxn struct {
	*regattapb.Command
}

func (c commandTxn) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	succ, rop, err := handleTxn(ctx, c.Txn.Compare, c.Txn.Success, c.Txn.Failure)
	if err != nil {
		return ResultFailure, nil, err
	}
	result := ResultSuccess
	if !succ {
		result = ResultFailure
	}
	return result, &regattapb.CommandResult{Revision: ctx.index, Responses: rop}, nil
}

// handleTxn handle transaction operation, returns if the operation succeeded (if success, or fail was applied) list or respective results and error.
func handleTxn(ctx *updateContext, compare []*regattapb.Compare, success, fail []*regattapb.RequestOp) (bool, []*regattapb.ResponseOp, error) {
	if err := ctx.EnsureIndexed(); err != nil {
		return false, nil, err
	}
	ok, err := txnCompare(ctx.batch, compare)
	if err != nil {
		return false, nil, err
	}
	if ok {
		res, err := handleTxnOps(ctx, success)
		return true, res, err
	}
	res, err := handleTxnOps(ctx, fail)
	return false, res, err
}

func handleTxnOps(ctx *updateContext, req []*regattapb.RequestOp) ([]*regattapb.ResponseOp, error) {
	var results []*regattapb.ResponseOp
	for _, op := range req {
		switch o := op.Request.(type) {
		case *regattapb.RequestOp_RequestRange:
			response, err := lookup(ctx.batch, o.RequestRange)
			if err != nil {
				return nil, err
			}
			results = append(results, wrapResponseOp(response))
		case *regattapb.RequestOp_RequestPut:
			response, err := handlePut(ctx, o.RequestPut)
			if err != nil {
				return nil, err
			}
			results = append(results, wrapResponseOp(response))
		case *regattapb.RequestOp_RequestDeleteRange:
			response, err := handleDelete(ctx, o.RequestDeleteRange)
			if err != nil {
				return nil, err
			}
			results = append(results, wrapResponseOp(response))
		}
	}
	return results, nil
}

func txnCompare(reader pebble.Reader, compare []*regattapb.Compare) (bool, error) {
	keyBuf := bufferPool.Get()
	defer bufferPool.Put(keyBuf)
	for _, cmp := range compare {
		var (
			res bool
			err error
		)
		if cmp.RangeEnd != nil {
			res, err = func() (bool, error) {
				opts, err := iterOptionsForBounds(cmp.Key, cmp.RangeEnd)
				if err != nil {
					return false, err
				}
				iter, err := reader.NewIter(opts)
				if err != nil {
					return false, err
				}
				defer func() {
					_ = iter.Close()
				}()
				if !iter.First() {
					return false, nil
				}
				for iter.First(); iter.Valid(); iter.Next() {
					if !txnCompareSingle(cmp, iter.Value()) {
						return false, nil
					}
				}
				return true, nil
			}()
		} else {
			res, err = func() (bool, error) {
				if err := encodeUserKey(keyBuf, cmp.Key); err != nil {
					return false, err
				}
				value, closer, err := reader.Get(keyBuf.Bytes())
				defer func() {
					if closer != nil {
						_ = closer.Close()
					}
				}()

				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						return false, nil
					}
					return false, err
				}

				if !txnCompareSingle(cmp, value) {
					return false, nil
				}

				keyBuf.Reset()
				return true, nil
			}()
		}
		if err != nil {
			return false, err
		}
		if !res {
			return false, nil
		}
	}
	return true, nil
}

func txnCompareSingle(cmp *regattapb.Compare, value []byte) bool {
	cmpValue := true
	if cmp.Target == regattapb.Compare_VALUE && cmp.TargetUnion != nil {
		switch cmp.Result {
		case regattapb.Compare_EQUAL:
			cmpValue = bytes.Equal(value, cmp.GetValue())
		case regattapb.Compare_NOT_EQUAL:
			cmpValue = !bytes.Equal(value, cmp.GetValue())
		case regattapb.Compare_GREATER:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == 1
		case regattapb.Compare_LESS:
			cmpValue = bytes.Compare(value, cmp.GetValue()) == -1
		}
	}

	return cmpValue
}
