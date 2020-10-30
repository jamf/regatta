package raft

import (
	"fmt"
	"io"
	"sync"
	"testing"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

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
			"Badger -> Badger",
			args{
				producingSMFactory: filledBadgerSM,
				receivingSMFactory: emptyBadgerSM,
			},
		},
		{
			"Pebble -> Badger",
			args{
				producingSMFactory: filledPebbleSM,
				receivingSMFactory: emptyBadgerSM,
			},
		},
		{
			"Badger -> Pebble",
			args{
				producingSMFactory: filledBadgerSM,
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

			pr, pw := io.Pipe()
			ep := tt.args.receivingSMFactory()
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
			got, err := ep.(sm.IHash).GetHash()
			r.NoError(err)
			r.Equal(want, got, "the hash of recovered DB should be the same as of the original one")
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
		{
			name: "Badger - Lookup empty DB",
			fields: fields{
				smFactory: emptyBadgerSM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Badger - Lookup full DB with non-existent key",
			fields: fields{
				smFactory: filledBadgerSM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Badger - Lookup full DB with existing key",
			fields: fields{
				smFactory: filledBadgerSM,
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
		{
			name: "Badger - Successful update of a single item",
			fields: fields{
				smFactory: emptyBadgerSM,
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
			name: "Badger - Successful update of a batch",
			fields: fields{
				smFactory: emptyBadgerSM,
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
