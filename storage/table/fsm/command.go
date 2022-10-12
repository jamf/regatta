package fsm

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

type updateContext struct {
	batch       *pebble.Batch
	wo          *pebble.WriteOptions
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
	return c.batch.Commit(c.wo)
}

func (c *updateContext) Close() error {
	if err := c.batch.Close(); err != nil {
		return err
	}
	return nil
}

func parseCommand(c *updateContext, entry sm.Entry) (command, error) {
	c.index = entry.Index
	cmd := &proto.Command{}
	if err := cmd.UnmarshalVT(entry.Cmd); err != nil {
		return commandDummy{}, err
	}
	c.leaderIndex = cmd.LeaderIndex
	switch cmd.Type {
	case proto.Command_PUT:
		return commandPut{cmd}, nil
	case proto.Command_DELETE:
		return commandDelete{cmd}, nil
	case proto.Command_PUT_BATCH:
		return commandPutBatch{cmd}, nil
	case proto.Command_DELETE_BATCH:
		return commandDeleteBatch{cmd}, nil
	case proto.Command_TXN:
		return commandTxn{cmd}, nil
	case proto.Command_DUMMY:
		return commandDummy{}, nil
	}
	return commandDummy{}, nil
}

type command interface {
	handle(*updateContext) (UpdateResult, *proto.CommandResult, error)
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
