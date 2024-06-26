// Copyright JAMF Software, LLC

package fsm

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/util"
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
				fs:          vfs.NewMem(),
				clusterID:   tt.fields.clusterID,
				nodeID:      tt.fields.nodeID,
				dirname:     tt.fields.dirname,
				log:         zap.NewNop().Sugar(),
				metrics:     newMetrics(testTable, tt.fields.clusterID),
				appliedFunc: func(applied uint64) {},
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

func TestFSM_ReOpen(t *testing.T) {
	r := require.New(t)
	fs := vfs.NewMem()
	const testIndex uint64 = 10
	p := &FSM{
		fs:          fs,
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp/dir",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
	}

	t.Log("open FSM")
	index, err := p.Open(nil)
	r.NoError(err)
	r.Equal(uint64(0), index)

	t.Log("propose into FSM")
	_, err = p.Update([]sm.Entry{
		{
			Index: testIndex,
			Cmd: mustMarshallProto(&regattapb.Command{
				Kv: &regattapb.KeyValue{
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
			name: "successful update of a single item",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
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
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful bump of a leader index",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DUMMY,
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
			name: "successful update of a batch",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &two,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE,
							Kv: &regattapb.KeyValue{
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
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 2,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 2,
		},
		{
			name: "successful update of single batched put",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT_BATCH,
							Batch: []*regattapb.KeyValue{
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
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful update of single batched delete",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE_BATCH,
							Batch: []*regattapb.KeyValue{
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
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful delete range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE,
							Kv:          &regattapb.KeyValue{Key: []byte("test")},
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
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 2,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 3,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful delete all range",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE,
							Kv:          &regattapb.KeyValue{Key: []byte("test")},
							RangeEnd:    prependByte([]byte("test"), 1),
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 2,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 3,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful delete single with count",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE,
							Kv:          &regattapb.KeyValue{Key: []byte("test1")},
							Count:       true,
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 2,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{Deleted: 1}}},
							},
						}),
					},
				},
			},
			wantLeaderIndex: 1,
		},
		{
			name: "successful delete all range with count",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{
				updates: []sm.Entry{
					{
						Index: 1,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test1"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 2,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_PUT,
							Kv: &regattapb.KeyValue{
								Key:   []byte("test2"),
								Value: []byte("test"),
							},
						}),
					},
					{
						Index: 3,
						Cmd: mustMarshallProto(&regattapb.Command{
							LeaderIndex: &one,
							Table:       []byte("test"),
							Type:        regattapb.Command_DELETE,
							Kv:          &regattapb.KeyValue{Key: []byte{0}},
							RangeEnd:    []byte{0},
							Count:       true,
						}),
					},
				},
			},
			want: []sm.Entry{
				{
					Index: 1,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 1,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 2,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 2,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponsePut{ResponsePut: &regattapb.ResponseOp_Put{}}},
							},
						}),
					},
				},
				{
					Index: 3,
					Result: sm.Result{
						Value: 1,
						Data: mustMarshallProto(&regattapb.CommandResult{
							Revision: 3,
							Responses: []*regattapb.ResponseOp{
								{Response: &regattapb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &regattapb.ResponseOp_DeleteRange{Deleted: 2}}},
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
				equalResult(t, tt.want[i].Result, entry.Result)
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

func equalResult(t *testing.T, want sm.Result, got sm.Result) {
	require.Equal(t, want.Value, got.Value, "value does not match")
	w := &regattapb.CommandResult{}
	g := &regattapb.CommandResult{}
	require.NoError(t, pb.Unmarshal(want.Data, w))
	require.NoError(t, pb.Unmarshal(got.Data, g))
	require.Equal(t, w, g, "data does not match")
}

func emptySM() *FSM {
	p := &FSM{
		fs:          vfs.NewMem(),
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp/tst",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
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
			Cmd: mustMarshallProto(&regattapb.Command{
				Table: []byte(testTable),
				Type:  regattapb.Command_PUT,
				Kv: &regattapb.KeyValue{
					Key:   []byte(fmt.Sprintf(testKeyFormat, i)),
					Value: []byte(testValue),
				},
			}),
		})
	}
	for i := 0; i < largeEntries; i++ {
		entries = append(entries, sm.Entry{
			Index: uint64(i),
			Cmd: mustMarshallProto(&regattapb.Command{
				Table: []byte(testTable),
				Type:  regattapb.Command_PUT,
				Kv: &regattapb.KeyValue{
					Key:   []byte(fmt.Sprintf(testLargeKeyFormat, i)),
					Value: []byte(largeValues[i]),
				},
			}),
		})
	}
	p := &FSM{
		fs:          vfs.NewMem(),
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp/tst",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
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
			Cmd: mustMarshallProto(&regattapb.Command{
				Table: []byte(testTable),
				Type:  regattapb.Command_PUT,
				Kv: &regattapb.KeyValue{
					Key:   []byte(fmt.Sprintf(testKeyFormat, i)),
					Value: []byte(largeValues[i]),
				},
			}),
		}
	}
	p := &FSM{
		fs:          vfs.NewMem(),
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp/tst",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
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
		fs:          vfs.NewMem(),
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp/tst",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
	}
	_, err := p.Open(nil)
	if err != nil {
		panic(err)
	}
	_, err = p.Update([]sm.Entry{
		{
			Index: uint64(1),
			Cmd: mustMarshallProto(&regattapb.Command{
				Table: []byte(testTable),
				Type:  regattapb.Command_DUMMY,
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
