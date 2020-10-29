package raft

import (
	"fmt"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
)

func emptyBadgerSM() sm.IOnDiskStateMachine {
	p := &KVBadgerStateMachine{
		clusterID: 1,
		nodeID:    1,
		dirname:   "",
		log:       zap.S(),
		inMemory:  true,
	}
	_, err := p.Open(nil)
	if err != nil {
		zap.S().Panic(err)
	}
	return p
}

func filledBadgerSM() sm.IOnDiskStateMachine {
	entries := make([]sm.Entry, 10_000)
	for i := 0; i < len(entries); i++ {
		entries[i] = sm.Entry{
			Index: uint64(i),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_PUT,
				Kv: &proto.KeyValue{
					Key:   []byte(fmt.Sprintf(testKeyFormat, i)),
					Value: []byte(testValue),
				},
			}),
		}
	}
	p := &KVBadgerStateMachine{
		clusterID: 1,
		nodeID:    1,
		dirname:   "",
		log:       zap.S(),
		inMemory:  true,
	}
	_, err := p.Open(nil)
	if err != nil {
		zap.S().Panic(err)
	}
	_, err = p.Update(entries)
	if err != nil {
		zap.S().Panic(err)
	}
	return p
}
