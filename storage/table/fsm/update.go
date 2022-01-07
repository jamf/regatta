package fsm

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
)

// Update updates the object.
func (p *FSM) Update(updates []sm.Entry) ([]sm.Entry, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	buf := bytes.NewBuffer(make([]byte, key.LatestVersionLen))

	batch := db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	cmd := proto.CommandFromVTPool()
	defer cmd.ReturnToVTPool()

	for i := 0; i < len(updates); i++ {
		cmd.ResetVT()
		err := cmd.UnmarshalVT(updates[i].Cmd)
		if err != nil {
			return nil, err
		}
		buf.Reset()

		switch cmd.Type {
		case proto.Command_PUT:
			if err := encodeUserKey(buf, cmd.Kv.Key); err != nil {
				return nil, err
			}
			if err := batch.Set(buf.Bytes(), cmd.Kv.Value, nil); err != nil {
				return nil, err
			}
		case proto.Command_DELETE:
			if err := encodeUserKey(buf, cmd.Kv.Key); err != nil {
				return nil, err
			}
			if err := batch.Delete(buf.Bytes(), nil); err != nil {
				return nil, err
			}
		case proto.Command_PUT_BATCH:
			for _, kv := range cmd.Batch {
				if err := encodeUserKey(buf, kv.Key); err != nil {
					return nil, err
				}
				if err := batch.Set(buf.Bytes(), kv.Value, nil); err != nil {
					return nil, err
				}
				buf.Reset()
			}
		case proto.Command_DELETE_BATCH:
			for _, kv := range cmd.Batch {
				if err := encodeUserKey(buf, kv.Key); err != nil {
					return nil, err
				}
				if err := batch.Delete(buf.Bytes(), nil); err != nil {
					return nil, err
				}
				buf.Reset()
			}
		case proto.Command_DUMMY:
		}
		updates[i].Result = sm.Result{Value: 1}
	}

	// Set leader index if present in the proposal
	if cmd.LeaderIndex != nil {
		leaderIdx := make([]byte, 8)
		binary.LittleEndian.PutUint64(leaderIdx, *cmd.LeaderIndex)
		if err := batch.Set(sysLeaderIndex, leaderIdx, nil); err != nil {
			return nil, err
		}
	}

	// Set local index
	idx := make([]byte, 8)
	binary.LittleEndian.PutUint64(idx, updates[len(updates)-1].Index)
	if err := batch.Set(sysLocalIndex, idx, nil); err != nil {
		return nil, err
	}

	if err := batch.Commit(p.wo); err != nil {
		return nil, err
	}
	return updates, nil
}
