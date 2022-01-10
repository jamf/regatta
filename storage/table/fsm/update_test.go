package fsm

import (
	"testing"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
)

func TestSM_Update(t *testing.T) {
	one := uint64(1)
	two := uint64(2)
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
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
					Cmd: mustMarshallProto(&proto.Command{
						LeaderIndex: &one,
						Table:       []byte("test"),
						Type:        proto.Command_PUT,
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
					Cmd: mustMarshallProto(&proto.Command{
						LeaderIndex: &one,
						Table:       []byte("test"),
						Type:        proto.Command_DUMMY,
					}),
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
					Cmd: mustMarshallProto(&proto.Command{
						LeaderIndex: &one,
						Table:       []byte("test"),
						Type:        proto.Command_PUT,
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
						LeaderIndex: &two,
						Table:       []byte("test"),
						Type:        proto.Command_DELETE,
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
					Result: sm.Result{
						Value: 1,
						Data:  nil,
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
					Result: sm.Result{
						Value: 1,
						Data:  nil,
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
			r.Equal(tt.want, got)

			// Test whether the leaderIndex has changed.
			res, err := p.Lookup(LeaderIndexRequest{})
			r.NoError(err)
			indexRes, ok := res.(*IndexResponse)
			if !ok {
				r.Fail("could not cast response to *IndexResponse")
			}
			r.Equal(indexRes.Index, tt.wantLeaderIndex)
		})
	}
}
