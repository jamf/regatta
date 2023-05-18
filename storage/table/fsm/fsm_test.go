// Copyright JAMF Software, LLC

package fsm

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/util"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
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

var (
	one = uint64(1)
	two = uint64(2)
)

func TestSM_Open(t *testing.T) {
	type fields struct {
		clusterID uint64
		nodeID    uint64
		dirname   string
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
				clusterID: 1,
				nodeID:    1,
				dirname:   "/tmp/dir",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := &FSM{
				fs:        vfs.NewMem(),
				clusterID: tt.fields.clusterID,
				nodeID:    tt.fields.nodeID,
				dirname:   tt.fields.dirname,
				log:       zap.NewNop().Sugar(),
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
		fs:        fs,
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp/dir",
		log:       zap.NewNop().Sugar(),
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

func TestSM_Update(t *testing.T) {
	type fields struct {
		smFactory func() *FSM
	}
	type args struct {
		updates []sm.Entry
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		want            []sm.Entry
		wantErr         bool
		wantLeaderIndex uint64
	}{
		{
			name: "Pebble - Successful update of a single item",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
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
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful bump of a leader index",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DUMMY,
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data:  nil,
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful update of a batch",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &two,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
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
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 2,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{Deleted: 1}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 2,
		},
		{
			name: "Pebble - Successful update of single batched PUT",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT_BATCH,
							Batch: []*proto.KeyValue{
								{
									Key:   []byte("test"),
									Value: []byte("test"),
								},
								{
									Key:   []byte("test1"),
									Value: []byte("test"),
								},
								{
									Key:   []byte("test2"),
									Value: []byte("test"),
								},
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful update of single batched DELETE",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE_BATCH,
							Batch: []*proto.KeyValue{
								{
									Key: []byte("test"),
								},
								{
									Key: []byte("test1"),
								},
								{
									Key: []byte("test2"),
								},
							},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}}},
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}}},
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful DELETE range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
							Kv:          &proto.KeyValue{Key: []byte("test")},
							RangeEnd:    incrementRightmostByte([]byte("test")),
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 2,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 3,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{Deleted: 2}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "Pebble - Successful DELETE all range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_PUT,
							Kv: &proto.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&proto.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        proto.Command_DELETE,
							Kv:          &proto.KeyValue{Key: []byte("test")},
							RangeEnd:    []byte{0},
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 1,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 2,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&proto.CommandResult{
							Revision: 3,
							Responses: []*proto.ResponseOp{
								{Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{Deleted: 2}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
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

			maxIndex := uint64(0)
			for i, entry := range got {
				r.Equal(tt.want[i].Index, entry.Index)
				r.Equal(tt.want[i].Result, entry.Result)
				maxIndex = entry.Index
			}

			// Test whether the index is stored.
			res, err := p.Lookup(LocalIndexRequest{})
			r.NoError(err)
			indexRes, ok := res.(*IndexResponse)
			if !ok {
				r.Fail("could not cast response to *IndexResponse")
			}
			r.Equal(indexRes.Index, maxIndex)

			// Test whether the leaderIndex has changed.
			res, err = p.Lookup(LeaderIndexRequest{})
			r.NoError(err)
			indexRes, ok = res.(*IndexResponse)
			if !ok {
				r.Fail("could not cast response to *IndexResponse")
			}
			r.Equal(tt.wantLeaderIndex, indexRes.Index)
		})
	}
}

func emptySM() *FSM {
	p := &FSM{
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp/tst",
		log:       zap.NewNop().Sugar(),
	}
	_, err := p.Open(nil)
	if err != nil {
		panic(err)
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
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp/tst",
		log:       zap.NewNop().Sugar(),
	}
	_, err := p.Open(nil)
	if err != nil {
		panic(err)
	}
	_, err = p.Update(entries)
	if err != nil {
		panic(err)
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
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp/tst",
		log:       zap.NewNop().Sugar(),
	}
	_, err := p.Open(nil)
	if err != nil {
		panic(err)
	}
	_, err = p.Update(entries)
	if err != nil {
		panic(err)
	}
	return p
}

func filledIndexOnlySM() *FSM {
	p := &FSM{
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		dirname:   "/tmp/tst",
		log:       zap.NewNop().Sugar(),
	}
	_, err := p.Open(nil)
	if err != nil {
		panic(err)
	}
	_, err = p.Update([]sm.Entry{
		{
			Index: uint64(1),
			Cmd: mustMarshallProto(&proto.Command{
				Table: []byte(testTable),
				Type:  proto.Command_DUMMY,
			}),
		},
	})
	if err != nil {
		panic(err)
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
		panic(err)
	}
	return bytes
}
