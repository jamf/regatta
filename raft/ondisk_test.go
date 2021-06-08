package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/util"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

var largeValues []string

func init() {
	for i := 0; i < 10_000; i++ {
		largeValues = append(largeValues, util.RandString(2048))
	}
}

func TestKVStateMachine_Snapshot(t *testing.T) {
	type args struct {
		producingSMFactory func() sm.IOnDiskStateMachine
		receivingSMFactory func() sm.IOnDiskStateMachine
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"Pebble -> Pebble",
			args{
				producingSMFactory: filledPebbleSM,
				receivingSMFactory: emptyPebbleSM,
			},
		},
		{
			"Pebble(large) -> Pebble",
			args{
				producingSMFactory: filledPebbleLargeValuesSM,
				receivingSMFactory: emptyPebbleSM,
			},
		},
	}
	for _, tt := range tests {
		t.Log("Applying snapshot to the empty DB should produce the same hash")
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.args.producingSMFactory()
			defer p.Close()

			want, err := p.(sm.IHash).GetHash()
			r.NoError(err)

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()
			defer ep.Close()

			snapf, err := os.Create(filepath.Join(t.TempDir(), "snapshot-file"))
			if err == nil {
				defer snapf.Close()
			}
			r.NoError(err)

			t.Log("Save snapshot started")
			err = p.SaveSnapshot(snp, snapf, nil)
			r.NoError(err)
			_, err = snapf.Seek(0, 0)
			r.NoError(err)

			t.Log("Recover from snapshot started")
			stopc := make(chan struct{})
			err = ep.RecoverFromSnapshot(snapf, stopc)
			r.NoError(err)

			t.Log("Recovery finished")

			got, err := ep.(sm.IHash).GetHash()
			r.NoError(err)
			r.Equal(want, got, "the hash of recovered DB should be the same as of the original one")
		})
	}
}

func TestKVStateMachine_Snapshot_Stopped(t *testing.T) {
	type args struct {
		producingSMFactory func() sm.IOnDiskStateMachine
		receivingSMFactory func() sm.IOnDiskStateMachine
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"Pebble(large) -> Pebble",
			args{
				producingSMFactory: filledPebbleLargeValuesSM,
				receivingSMFactory: emptyPebbleSM,
			},
		},
	}
	for _, tt := range tests {
		t.Log("Applying snapshot to the empty DB should be stopped")
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.args.producingSMFactory()
			defer p.Close()

			snp, err := p.PrepareSnapshot()
			r.NoError(err)

			ep := tt.args.receivingSMFactory()

			snapf, err := os.Create(filepath.Join(t.TempDir(), "snapshot-file"))
			r.NoError(err)
			t.Log("Save snapshot started")
			err = p.SaveSnapshot(snp, snapf, nil)
			r.NoError(err)
			_, err = snapf.Seek(0, 0)
			r.NoError(err)

			stopc := make(chan struct{})
			go func() {
				defer func() {
					_ = snapf.Close()
					_ = ep.Close()
					_ = snapf.Close()
				}()
				t.Log("Recover from snapshot routine started")
				err := ep.RecoverFromSnapshot(snapf, stopc)
				r.Error(err)
				r.Equal(sm.ErrSnapshotStopped, err)
			}()

			time.Sleep(10 * time.Millisecond)
			close(stopc)

			t.Log("Recovery stopped")
		})
	}
}

func TestKVStateMachine_Lookup(t *testing.T) {
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
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
			name: "Pebble - Lookup empty DB",
			fields: fields{
				smFactory: emptyPebbleSM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Pebble - Lookup full DB with non-existent key",
			fields: fields{
				smFactory: filledPebbleSM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Pebble - Lookup full DB with existing key",
			fields: fields{
				smFactory: filledPebbleSM,
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
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestKVStateMachine_Update(t *testing.T) {
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
	}
	type args struct {
		updates []sm.Entry
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []sm.Entry
		wantErr bool
	}{
		{
			name: "Pebble - Successful update of a single item",
			fields: fields{
				smFactory: emptyPebbleSM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
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
					Cmd: mustMarshallProto(&proto.Command{
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
			name: "Pebble - Successful update of a batch",
			fields: fields{
				smFactory: emptyPebbleSM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
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
						Cmd: mustMarshallProto(&proto.Command{
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
					Cmd: mustMarshallProto(&proto.Command{
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
					Cmd: mustMarshallProto(&proto.Command{
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
			p := tt.fields.smFactory()
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

func mustMarshallProto(message pb.Message) []byte {
	bytes, err := pb.Marshal(message)
	if err != nil {
		zap.S().Panic(err)
	}
	return bytes
}
