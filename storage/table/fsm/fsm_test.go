package fsm

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/util"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

const (
	testValue          = "test"
	testTable          = "test"
	testKeyFormat      = "test%d"
	testLargeKeyFormat = "testlarge%d"

	smallEntries = 10_000
	largeEntries = 10
)

func TestSM_Open(t *testing.T) {
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
			p := &FSM{
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

func TestSMReOpen(t *testing.T) {
	r := require.New(t)
	fs := vfs.NewMem()
	const testIndex uint64 = 10
	p := &FSM{
		fs:         fs,
		clusterID:  1,
		nodeID:     1,
		dirname:    "/tmp/dir",
		walDirname: "/tmp/dir",
		log:        zap.S(),
	}

	t.Log("open FSM")
	index, err := p.Open(nil)
	r.NoError(err)
	r.Equal(uint64(0), index)

	t.Log("propose into FSM")
	_, err = p.Update([]sm.Entry{
		{
			Index: testIndex,
			Cmd: mustMarshallProto(&proto.Command{
				Kv: &proto.KeyValue{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			}),
		},
	})
	r.NoError(err)
	r.NoError(p.Close())

	t.Log("reopen FSM")
	index, err = p.Open(nil)
	r.NoError(err)
	r.Equal(testIndex, index)
}

func emptySM() *FSM {
	p := &FSM{
		fs:         vfs.NewMem(),
		clusterID:  1,
		nodeID:     1,
		dirname:    "/tmp/tst",
		walDirname: "/tmp/tst",
		log:        zap.S(),
	}
	_, err := p.Open(nil)
	if err != nil {
		zap.S().Panic(err)
	}
	return p
}

func filledSM() *FSM {
	entries := make([]sm.Entry, 0, smallEntries+largeEntries)
	for i := 0; i < smallEntries; i++ {
		entries = append(entries, sm.Entry{
			Index: uint64(i),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_PUT,
				Kv: &proto.KeyValue{
					Key:   []byte(fmt.Sprintf(testKeyFormat, i)),
					Value: []byte(testValue),
				},
			}),
		})
	}
	for i := 0; i < largeEntries; i++ {
		entries = append(entries, sm.Entry{
			Index: uint64(i),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_PUT,
				Kv: &proto.KeyValue{
					Key:   []byte(fmt.Sprintf(testLargeKeyFormat, i)),
					Value: []byte(largeValues[i]),
				},
			}),
		})
	}
	p := &FSM{
		fs:         vfs.NewMem(),
		clusterID:  1,
		nodeID:     1,
		dirname:    "/tmp/tst",
		walDirname: "/tmp/tst",
		log:        zap.S(),
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

func filledLargeValuesSM() *FSM {
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
	p := &FSM{
		fs:         vfs.NewMem(),
		clusterID:  1,
		nodeID:     1,
		dirname:    "/tmp/tst",
		walDirname: "/tmp/tst",
		log:        zap.S(),
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

var largeValues []string

func init() {
	for i := 0; i < 10_000; i++ {
		largeValues = append(largeValues, util.RandString(10*1048))
	}
}

func mustMarshallProto(message pb.Message) []byte {
	bytes, err := pb.Marshal(message)
	if err != nil {
		zap.S().Panic(err)
	}
	return bytes
}
