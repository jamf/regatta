package fsm

import (
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/wandera/regatta/proto"
)

// Update advances the FSM.
func (p *FSM) Update(updates []sm.Entry) ([]sm.Entry, error) {
	db := p.pebble.Load()

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

		var cmd command
		switch ctx.cmd.Type {
		case proto.Command_PUT:
			cmd = commandPut{ctx}
		case proto.Command_DELETE:
			cmd = commandDelete{ctx}
		case proto.Command_PUT_BATCH:
			cmd = commandPutBatch{ctx}
		case proto.Command_DELETE_BATCH:
			cmd = commandDeleteBatch{ctx}
		case proto.Command_TXN:
			cmd = commandTxn{ctx}
		case proto.Command_DUMMY:
			cmd = commandDummy{ctx}
		}

		updateResult, res, err := cmd.handle()
		if err != nil {
			return nil, err
		}

		if len(res.Responses) > 0 {
			bts, err := res.MarshalVT()
			if err != nil {
				return nil, err
			}
			updates[i].Result.Data = bts
		}
		updates[i].Result.Value = uint64(updateResult)
	}

	if err := ctx.Commit(); err != nil {
		return nil, err
	}

	return updates, nil
}
