package raft

import (
	"fmt"
	"testing"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/util"
	"go.uber.org/zap"
)

func TestKVBadgerStateMachine_LargeValueLookup(t *testing.T) {
	r := require.New(t)
	expValue := util.RandString(2048)
	expKey := fmt.Sprintf(testKeyFormat, 0)
	entries := []sm.Entry{
		{
			Index: uint64(1),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_PUT,
				Kv: &proto.KeyValue{
					Key:   []byte(expKey),
					Value: []byte(expValue),
				},
			}),
		},
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
		r.NoError(err)
	}
	defer func() {
		r.NoError(p.Close())
	}()
	_, err = p.Update(entries)
	if err != nil {
		r.NoError(err)
	}

	t.Run("Lookup large value", func(t *testing.T) {
		r := require.New(t)
		got, err := p.Lookup(&proto.RangeRequest{
			Table: []byte(testTable),
			Key:   []byte(expKey),
		})
		r.NoError(err)
		r.Equal([]byte(expValue), got.(*proto.RangeResponse).Kvs[0].Value)
	})
}

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

func filledBadgerLargeValuesSM() sm.IOnDiskStateMachine {
	entries := make([]sm.Entry, len(largeValues))
	for i := 0; i < len(entries); i++ {
		entries[i] = sm.Entry{
			Index: uint64(i),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_PUT,
				Kv: &proto.KeyValue{
					Key:   []byte(fmt.Sprintf(testKeyFormat, i)),
					Value: []byte(largeValues[i]),
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
