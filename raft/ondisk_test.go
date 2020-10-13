package raft

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
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

func TestKVPebbleStateMachine_Lookup(t *testing.T) {
	type fields struct {
		sm *KVPebbleStateMachine
	}
	type args struct {
		key *proto.RangeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Lookup empty DB",
			fields: fields{
				sm: emptyPebbleSM(),
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Lookup full DB with non-existent key",
			fields: fields{
				sm: filledPebbleSM(),
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Lookup full DB with existing key",
			fields: fields{
				sm: filledPebbleSM(),
			},
			args: args{key: &proto.RangeRequest{
				Table: []byte(testTable),
				Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
			}},
			want: &proto.RangeResponse{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
						Value: []byte(testValue),
					},
				},
				Count: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			defer func() {
				r.NoError(tt.fields.sm.Close())
			}()
			got, err := tt.fields.sm.Lookup(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func MustMarshallProto(message pb.Message) []byte {
	bytes, err := pb.Marshal(message)
	if err != nil {
		zap.S().Panic(err)
	}
	return bytes
}

func TestKVPebbleStateMachine_Update(t *testing.T) {
	type args struct {
		updates []sm.Entry
	}
	tests := []struct {
		name    string
		args    args
		want    []sm.Entry
		wantErr bool
	}{
		{
			name: "Successful update of a single item",
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: MustMarshallProto(&proto.Command{
							Table: []byte("test"),
							Type:  proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Cmd: MustMarshallProto(&proto.Command{
						Table: []byte("test"),
						Type:  proto.Command_PUT,
						Kv: &proto.KeyValue{
							Key:   []byte("test"),
							Value: []byte("test"),
						},
					}),
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
		},
		{
			name: "Successful update of a batch",
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: MustMarshallProto(&proto.Command{
							Table: []byte("test"),
							Type:  proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: MustMarshallProto(&proto.Command{
							Table: []byte("test"),
							Type:  proto.Command_DELETE,
							Kv: &proto.KeyValue{
								Key: []byte("test"),
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Cmd: MustMarshallProto(&proto.Command{
						Table: []byte("test"),
						Type:  proto.Command_PUT,
						Kv: &proto.KeyValue{
							Key:   []byte("test"),
							Value: []byte("test"),
						},
					}),
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
				{
					Index: 2,
					Cmd: MustMarshallProto(&proto.Command{
						Table: []byte("test"),
						Type:  proto.Command_DELETE,
						Kv: &proto.KeyValue{
							Key: []byte("test"),
						},
					}),
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := emptyPebbleSM()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Update(tt.args.updates)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func TestKVPebbleStateMachine_Snapshot(t *testing.T) {
	t.Run("Applying snapshot to the empty DB should produce the same hash", func(t *testing.T) {
		r := require.New(t)
		p := filledPebbleSM()
		defer p.Close()

		want, err := p.GetHash()
		r.NoError(err)

		snp, err := p.PrepareSnapshot()
		r.NoError(err)

		pr, pw := io.Pipe()
		ep := emptyPebbleSM()
		defer ep.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Log("Save snapshot routine started")
			err := p.SaveSnapshot(snp, pw, nil)
			r.NoError(err)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Log("Recover from snapshot routine started")
			err := ep.RecoverFromSnapshot(pr, nil)
			r.NoError(err)
		}()

		wg.Wait()
		t.Log("Recovery finished")
		got, err := p.GetHash()
		r.NoError(err)
		r.Equal(want, got, "the hash of recovered DB should be the same as of the original one")
	})
}

func emptyPebbleSM() *KVPebbleStateMachine {
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

func filledPebbleSM() *KVPebbleStateMachine {
	entries := make([]sm.Entry, 10_000)
	for i := 0; i < len(entries); i++ {
		entries[i] = sm.Entry{
			Index: uint64(i),
			Cmd: MustMarshallProto(&proto.Command{
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
