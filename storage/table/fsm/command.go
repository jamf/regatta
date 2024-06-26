// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/regattapb"
	pb "google.golang.org/protobuf/proto"
)

type updateContext struct {
	batch       *pebble.Batch
	db          *pebble.DB
	index       uint64
	leaderIndex *uint64
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

func (c *updateContext) Commit() error {
	// Set leader index if present in the proposal
	if c.leaderIndex != nil {
		leaderIdx := make([]byte, 8)
		binary.LittleEndian.PutUint64(leaderIdx, *c.leaderIndex)
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
	return c.batch.Commit(pebble.NoSync)
}

func (c *updateContext) Close() error {
	if err := c.batch.Close(); err != nil {
		return err
	}
	return nil
}

func parseCommand(c *updateContext, entry sm.Entry) (command, error) {
	c.index = entry.Index
	cmd := &regattapb.Command{}
	if err := cmd.UnmarshalVTUnsafe(entry.Cmd); err != nil {
		return commandDummy{}, err
	}
	c.leaderIndex = cmd.LeaderIndex
	return wrapCommand(cmd), nil
}

func wrapCommand(cmd *regattapb.Command) command {
	switch cmd.Type {
	case regattapb.Command_PUT:
		return commandPut{cmd}
	case regattapb.Command_DELETE:
		return commandDelete{cmd}
	case regattapb.Command_PUT_BATCH:
		return commandPutBatch{cmd}
	case regattapb.Command_DELETE_BATCH:
		return commandDeleteBatch{cmd}
	case regattapb.Command_TXN:
		return commandTxn{cmd}
	case regattapb.Command_SEQUENCE:
		return commandSequence{cmd}
	case regattapb.Command_DUMMY:
		return commandDummy{}
	}
	panic("unknown command type")
}

type command interface {
	handle(*updateContext) (UpdateResult, *regattapb.CommandResult, error)
}

func wrapRequestOp(req pb.Message) *regattapb.RequestOp {
	switch op := req.(type) {
	case *regattapb.RequestOp_Range:
		return &regattapb.RequestOp{Request: &regattapb.RequestOp_RequestRange{RequestRange: op}}
	case *regattapb.RequestOp_Put:
		return &regattapb.RequestOp{Request: &regattapb.RequestOp_RequestPut{RequestPut: op}}
	case *regattapb.RequestOp_DeleteRange:
		return &regattapb.RequestOp{Request: &regattapb.RequestOp_RequestDeleteRange{RequestDeleteRange: op}}
	}
	return nil
}

func wrapResponseOp(req pb.Message) *regattapb.ResponseOp {
	switch op := req.(type) {
	case *regattapb.ResponseOp_Range:
		return &regattapb.ResponseOp{Response: &regattapb.ResponseOp_ResponseRange{ResponseRange: op}}
	case *regattapb.ResponseOp_Put:
		return &regattapb.ResponseOp{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: op}}
	case *regattapb.ResponseOp_DeleteRange:
		return &regattapb.ResponseOp{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: op}}
	}
	return nil
}
