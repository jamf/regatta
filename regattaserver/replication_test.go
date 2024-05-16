// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jamf/regatta/raft/raftpb"
	"github.com/jamf/regatta/regattapb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
)

func TestMetadataServer_Get(t *testing.T) {
	type fields struct {
		Tables []string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *regattapb.MetadataResponse
		wantErr error
	}{
		{
			name: "Get metadata - no tables",
			want: &regattapb.MetadataResponse{Tables: nil},
		},
		{
			name: "Get metadata - single table",
			fields: fields{
				Tables: []string{"foo"},
			},
			want: &regattapb.MetadataResponse{Tables: []*regattapb.Table{
				{
					Name: "foo",
					Type: regattapb.Table_REPLICATED,
				},
			}},
		},
		{
			name: "Get metadata - multiple tables",
			fields: fields{
				Tables: []string{"foo", "bar"},
			},
			want: &regattapb.MetadataResponse{Tables: []*regattapb.Table{
				{
					Name: "bar",
					Type: regattapb.Table_REPLICATED,
				},
				{
					Name: "foo",
					Type: regattapb.Table_REPLICATED,
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			m := &MetadataServer{
				Tables: newInMemTestEngine(t, tt.fields.Tables...),
			}
			got, err := m.Get(context.TODO(), &regattapb.MetadataRequest{})
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func TestEntryToCommand(t *testing.T) {
	zero := uint64(0)
	tests := []struct {
		name    string
		entry   raftpb.Entry
		wantCmd *regattapb.Command
		wantErr error
	}{
		{
			name:    "ConfigChange Entry Type",
			entry:   raftpb.Entry{Type: raftpb.ConfigChangeEntry, Index: 0},
			wantCmd: &regattapb.Command{Type: regattapb.Command_DUMMY, LeaderIndex: &zero},
			wantErr: nil,
		},
		{
			name: "Valid Entry",
			entry: raftpb.Entry{
				Type: raftpb.EncodedEntry,
				Cmd:  []byte{0, 10, 12, 114, 101, 103, 97, 116, 116, 97, 45, 116, 101, 115, 116, 26, 23, 10, 12, 49, 54, 50, 56, 48, 48, 50, 54, 52, 57, 95, 48, 34, 7, 118, 97, 108, 117, 101, 95, 48},
			},
			wantCmd: &regattapb.Command{
				Kv: &regattapb.KeyValue{
					Key:   []byte("1628002649_0"),
					Value: []byte("value_0"),
				},
				Table:       []byte("regatta-test"),
				LeaderIndex: &zero,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			gotCmd, gotErr := entryToCommand(tt.entry)
			if tt.wantErr == nil {
				r.NoError(gotErr)
			} else {
				r.Error(gotErr)
			}

			r.Equal(tt.wantCmd.LeaderIndex, gotCmd.LeaderIndex)
			r.Equal(tt.wantCmd.Table, gotCmd.Table)
			r.Equal(tt.wantCmd.Type, gotCmd.Type)
			if tt.wantCmd.Kv != nil {
				r.Equal(tt.wantCmd.Kv.Value, gotCmd.Kv.Value)
				r.Equal(tt.wantCmd.Kv.Key, gotCmd.Kv.Key)
			}
		})
	}
}

type captureSnapshotStream struct {
	grpc.ServerStream
	chunks []*regattapb.SnapshotChunk
}

func (c *captureSnapshotStream) Context() context.Context {
	return context.TODO()
}

func (c *captureSnapshotStream) Send(chunk *regattapb.SnapshotChunk) error {
	c.chunks = append(c.chunks, chunk)
	return nil
}

func TestSnapshotServer_Stream(t *testing.T) {
	type fields struct {
		Tables []string
	}
	type args struct {
		req *regattapb.SnapshotRequest
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantErr         require.ErrorAssertionFunc
		wantChunksCount int
	}{
		{
			name:    "table not exist",
			args:    args{req: &regattapb.SnapshotRequest{Table: table1Name}},
			wantErr: require.Error,
		},
		{
			name:            "snapshot",
			fields:          fields{Tables: []string{string(table1Name)}},
			args:            args{req: &regattapb.SnapshotRequest{Table: table1Name}},
			wantErr:         require.NoError,
			wantChunksCount: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SnapshotServer{
				Tables: newInMemTestEngine(t, tt.fields.Tables...),
			}
			capture := &captureSnapshotStream{}
			tt.wantErr(t, s.Stream(tt.args.req, capture), fmt.Sprintf("Stream(%v)", tt.args.req))
			require.Len(t, capture.chunks, tt.wantChunksCount)
		})
	}
}

type captureLogStream struct {
	grpc.ServerStream
	ctx    context.Context
	stream []*regattapb.ReplicateResponse
}

func (c *captureLogStream) Context() context.Context {
	return c.ctx
}

func (c *captureLogStream) Send(r *regattapb.ReplicateResponse) error {
	c.stream = append(c.stream, r)
	return nil
}

func TestLogServer_Replicate(t *testing.T) {
	type fields struct {
		Tables         []string
		maxMessageSize uint64
	}
	type args struct {
		req *regattapb.ReplicateRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not exist",
			args:    args{req: &regattapb.ReplicateRequest{Table: table1Name}},
			wantErr: require.Error,
		},
		{
			name:    "zero index",
			args:    args{req: &regattapb.ReplicateRequest{Table: table1Name}},
			wantErr: require.Error,
		},
		{
			name:    "stream",
			fields:  fields{Tables: []string{string(table1Name)}},
			args:    args{req: &regattapb.ReplicateRequest{Table: table1Name, LeaderIndex: 1}},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := newInMemTestEngine(t, tt.fields.Tables...)
			l := &LogServer{
				Tables:         te,
				LogReader:      te.LogReader,
				Log:            zaptest.NewLogger(t).Sugar(),
				maxMessageSize: tt.fields.maxMessageSize,
			}
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()
			stream := &captureLogStream{ctx: ctx}
			tt.wantErr(t, l.Replicate(tt.args.req, stream), fmt.Sprintf("Replicate(%v)", tt.args.req))
		})
	}
}
