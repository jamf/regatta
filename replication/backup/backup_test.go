// Copyright JAMF Software, LLC

package backup

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"github.com/jamf/regatta/log"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func init() {
	logger.SetLoggerFactory(log.LoggerFactory(zap.NewNop()))
}

func TestBackup_Backup(t *testing.T) {
	type fields struct {
		Timeout time.Duration
	}
	tests := []struct {
		name      string
		fields    fields
		tableData map[string][]*proto.PutRequest
		want      Manifest
		wantErr   error
	}{
		{
			name: "No tables backup",
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables:   nil,
			},
		},
		{
			name: "Single empty table backup",
			tableData: map[string][]*proto.PutRequest{
				"regatta-test": nil,
			},
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables: []ManifestTable{
					{
						Name:     "regatta-test",
						Type:     "REPLICATED",
						FileName: "regatta-test.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
				},
			},
		},
		{
			name: "Multiple empty table backup",
			tableData: map[string][]*proto.PutRequest{
				"regatta-test":  nil,
				"regatta-test2": nil,
			},
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables: []ManifestTable{
					{
						Name:     "regatta-test",
						Type:     "REPLICATED",
						FileName: "regatta-test.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
					{
						Name:     "regatta-test2",
						Type:     "REPLICATED",
						FileName: "regatta-test2.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
				},
			},
		},
		{
			name: "Multiple table backup",
			tableData: map[string][]*proto.PutRequest{
				"regatta-test": {
					&proto.PutRequest{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
				"regatta-test2": {
					&proto.PutRequest{
						Key:   []byte("foo2"),
						Value: []byte("bar2"),
					},
				},
			},
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables: []ManifestTable{
					{
						Name:     "regatta-test",
						Type:     "REPLICATED",
						FileName: "regatta-test.bak",
						MD5:      "5cc50dc8f85f6ab733c9cff534a398dd",
					},
					{
						Name:     "regatta-test2",
						Type:     "REPLICATED",
						FileName: "regatta-test2.bak",
						MD5:      "df74e3ebc3b31f884245cf0efb3c9b6e",
					},
				},
			},
		},
		{
			name: "Multiple table backup timeout",
			fields: fields{
				Timeout: 1 * time.Microsecond,
			},
			tableData: map[string][]*proto.PutRequest{
				"regatta-test": {
					&proto.PutRequest{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
				"regatta-test2": {
					&proto.PutRequest{
						Key:   []byte("foo2"),
						Value: []byte("bar2"),
					},
				},
			},
			wantErr: status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			nh, nodes, err := startRaftNode()
			r.NoError(err)
			defer nh.Close()
			tm := tables.NewManager(nh, nodes, tables.Config{
				NodeID: 1,
				Table:  tables.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024},
				Meta:   tables.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
			})
			r.NoError(tm.Start())
			r.NoError(tm.WaitUntilReady())
			defer tm.Close()

			for name, data := range tt.tableData {
				r.NoError(tm.CreateTable(name))
				time.Sleep(1 * time.Second)
				for _, req := range data {
					tbl, err := tm.GetTable(name)
					r.NoError(err)
					func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_, err := tbl.Put(ctx, req)
						r.NoError(err)
					}()
				}
			}

			srv := startBackupServer(tm)
			conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			r.NoError(err)

			path := filepath.Join(t.TempDir(), strings.ReplaceAll(tt.name, " ", "_"))
			r.NoError(os.MkdirAll(path, 0o777))
			b := &Backup{
				Conn:    conn,
				Dir:     path,
				Timeout: tt.fields.Timeout,
				clock:   clock.NewMock(),
			}
			got, err := b.Backup()
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestBackup_Restore(t *testing.T) {
	type fields struct {
		Timeout time.Duration
		Dir     string
	}
	tests := []struct {
		name       string
		fields     fields
		wantErrStr string
	}{
		{
			name: "Restore backup",
			fields: fields{
				Dir: "testdata/backup",
			},
		},
		{
			name: "Restore empty",
			fields: fields{
				Dir: "testdata/backup-empty",
			},
		},
		{
			name: "Restore corrupted",
			fields: fields{
				Dir: "testdata/backup-corrupted",
			},
			wantErrStr: "checksum mismatch",
		},
		{
			name: "Restore missing file",
			fields: fields{
				Dir: "testdata/backup-missing-file",
			},
			wantErrStr: "no such file or directory",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			nh, nodes, err := startRaftNode()
			r.NoError(err)
			defer nh.Close()
			tm := tables.NewManager(nh, nodes, tables.Config{
				NodeID: 1,
				Table:  tables.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024},
				Meta:   tables.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
			})
			r.NoError(tm.Start())
			r.NoError(tm.WaitUntilReady())
			defer tm.Close()

			srv := startBackupServer(tm)
			conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			r.NoError(err)

			b := &Backup{
				Conn:    conn,
				Dir:     tt.fields.Dir,
				Timeout: tt.fields.Timeout,
				clock:   clock.NewMock(),
			}
			err = b.Restore()
			if tt.wantErrStr != "" {
				r.Contains(err.Error(), tt.wantErrStr)
				return
			}
			r.NoError(err)
		})
	}
}

func TestBackup_ensureDefaults(t *testing.T) {
	type fields struct {
		Conn    *grpc.ClientConn
		Log     Logger
		Timeout time.Duration
		Dir     string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "Log defaults to nilLogger",
			fields: fields{
				Log:     nil,
				Timeout: 1 * time.Second,
			},
		},
		{
			name: "Timeout defaults to 1hr",
			fields: fields{
				Log:     nilLogger{},
				Timeout: 0,
			},
		},
		{
			name:   "Default all values",
			fields: fields{},
		},
		{
			name: "No defaulting",
			fields: fields{
				Log:     nilLogger{},
				Timeout: 1 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			b := &Backup{
				Conn:    tt.fields.Conn,
				Log:     tt.fields.Log,
				Timeout: tt.fields.Timeout,
				Dir:     tt.fields.Dir,
			}
			b.ensureDefaults()
			if tt.fields.Log == nil {
				r.Equal(nilLogger{}, b.Log)
			}
			if tt.fields.Timeout == 0 {
				r.Equal(1*time.Hour, b.Timeout)
			}
		})
	}
}

func startRaftNode() (*dragonboat.NodeHost, map[uint64]string, error) {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 1,
		RaftAddress:    testNodeAddress,
	}
	_ = nhc.Prepare()
	nhc.Expert.FS = vfs.NewMem()
	nhc.Expert.Engine.ExecShards = 1
	nhc.Expert.LogDB.Shards = 1
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, nil, err
	}
	return nh, map[uint64]string{1: testNodeAddress}, nil
}

func startBackupServer(manager *tables.Manager) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	proto.RegisterMetadataServer(server, &regattaserver.MetadataServer{Tables: manager})
	proto.RegisterMaintenanceServer(server, &regattaserver.BackupServer{Tables: manager})
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	// Let the server start.
	time.Sleep(100 * time.Millisecond)
	return server
}

func getTestPort() int {
	l, _ := net.Listen("tcp", ":0")
	defer func() {
		_ = l.Close()
	}()
	return l.Addr().(*net.TCPAddr).Port
}
