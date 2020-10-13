package raft

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	proto2 "google.golang.org/protobuf/proto"
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
		clusterID  uint64
		nodeID     uint64
		dirname    string
		walDirname string
	}
	type args struct {
		key interface{}
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
				clusterID: 1,
				nodeID:    1,
				dirname:   "/tmp/dir",
			},
			args:    args{key: []byte("Hello")},
			wantErr: true,
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
			r.NoError(err)
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func MustMarshallProto(message proto2.Message) []byte {
	bytes, _ := proto2.Marshal(message)
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
			p := &KVPebbleStateMachine{
				fs:        vfs.NewMem(),
				clusterID: 1,
				nodeID:    1,
				dirname:   "/tmp/dir",
				log:       zap.S(),
			}
			_, err := p.Open(nil)
			r.NoError(err)
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
	type fields struct {
		clusterID  uint64
		nodeID     uint64
		dirname    string
		walDirname string
	}
	type args struct {
		r   io.Reader
		in1 <-chan struct{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &KVPebbleStateMachine{
				fs:         vfs.NewMem(),
				clusterID:  tt.fields.clusterID,
				nodeID:     tt.fields.nodeID,
				dirname:    tt.fields.dirname,
				walDirname: tt.fields.walDirname,
				log:        zap.S(),
			}
			if err := p.RecoverFromSnapshot(tt.args.r, tt.args.in1); (err != nil) != tt.wantErr {
				t.Errorf("RecoverFromSnapshot() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
