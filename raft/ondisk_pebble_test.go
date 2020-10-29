package raft

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
)

const (
	testValue     = "test"
	testTable     = "test"
	testKeyFormat = "test%d"
)

func TestKVPebbleStateMachine_Open(t *testing.T) {
	type fields struct {
		clusterID  uint64
		nodeID     uint64
		dirname    string
		walDirname string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Invalid node ID",
			fields: fields{
				clusterID: 1,
				nodeID:    0,
				dirname:   "/tmp/dir",
			},
			wantErr: true,
		},
		{
			name: "Invalid cluster ID",
			fields: fields{
				clusterID: 0,
				nodeID:    1,
				dirname:   "/tmp/dir",
			},
			wantErr: true,
		},
		{
			name: "Successfully open DB",
			fields: fields{
				clusterID: 1,
				nodeID:    1,
				dirname:   "/tmp/dir",
			},
		},
		{
			name: "Successfully open DB with WAL",
			fields: fields{
				clusterID:  1,
				nodeID:     1,
				dirname:    "/tmp/dir",
				walDirname: "/tmp/waldir",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := &KVPebbleStateMachine{
				fs:         vfs.NewMem(),
				clusterID:  tt.fields.clusterID,
				nodeID:     tt.fields.nodeID,
				dirname:    tt.fields.dirname,
				walDirname: tt.fields.walDirname,
				log:        zap.S(),
			}
			_, err := p.Open(nil)
			if tt.wantErr {
				r.Error(err)
			} else {
				r.NoError(err)
				r.NoError(p.Close())
			}
		})
	}
}

func emptyPebbleSM() sm.IOnDiskStateMachine {
	p := &KVPebbleStateMachine{
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp",
		log:       zap.S(),
	}
	_, err := p.Open(nil)
	if err != nil {
		zap.S().Panic(err)
	}
	return p
}

func filledPebbleSM() sm.IOnDiskStateMachine {
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
	p := &KVPebbleStateMachine{
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp",
		log:       zap.S(),
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
