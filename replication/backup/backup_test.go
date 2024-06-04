// Copyright JAMF Software, LLC

package backup

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage"
	lvfs "github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestBackup_Backup(t *testing.T) {
	type fields struct {
		Timeout time.Duration
	}
	tests := []struct {
		name      string
		fields    fields
		tableData map[string][]*regattapb.PutRequest
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
			tableData: map[string][]*regattapb.PutRequest{
				"regatta-test": nil,
			},
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables: []ManifestTable{
					{
						Name:     "regatta-test",
						FileName: "regatta-test.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
				},
			},
		},
		{
			name: "Multiple empty table backup",
			tableData: map[string][]*regattapb.PutRequest{
				"regatta-test":  nil,
				"regatta-test2": nil,
			},
			want: Manifest{
				Started:  time.Unix(0, 0),
				Finished: time.Unix(0, 0),
				Tables: []ManifestTable{
					{
						Name:     "regatta-test",
						FileName: "regatta-test.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
					{
						Name:     "regatta-test2",
						FileName: "regatta-test2.bak",
						MD5:      "d41d8cd98f00b204e9800998ecf8427e",
					},
				},
			},
		},
		{
			name: "Multiple table backup",
			tableData: map[string][]*regattapb.PutRequest{
				"regatta-test": {
					&regattapb.PutRequest{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
				"regatta-test2": {
					&regattapb.PutRequest{
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
						FileName: "regatta-test.bak",
						MD5:      "5cc50dc8f85f6ab733c9cff534a398dd",
					},
					{
						Name:     "regatta-test2",
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
			tableData: map[string][]*regattapb.PutRequest{
				"regatta-test": {
					&regattapb.PutRequest{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
				"regatta-test2": {
					&regattapb.PutRequest{
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

			e := newTestEngine(t)
			defer e.Close()

			for name, data := range tt.tableData {
				_, err := e.CreateTable(name)
				r.NoError(err)
				time.Sleep(1 * time.Second)
				for _, req := range data {
					tbl, err := e.GetTable(name)
					r.NoError(err)
					func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_, err := tbl.Put(ctx, req)
						r.NoError(err)
					}()
				}
			}

			srv := startBackupServer(e)
			conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

			e := newTestEngine(t)
			defer e.Close()

			srv := startBackupServer(e)
			conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func newTestConfig(t *testing.T) storage.Config {
	fs := lvfs.NewMem()
	raftPort := getTestPort()
	gossipPort := getTestPort()
	return storage.Config{
		Log:            zaptest.NewLogger(t).Sugar(),
		NodeID:         1,
		InitialMembers: map[uint64]string{1: fmt.Sprintf("127.0.0.1:%d", raftPort)},
		WALDir:         "/wal",
		NodeHostDir:    "/nh",
		RTTMillisecond: 5,
		RaftAddress:    fmt.Sprintf("127.0.0.1:%d", raftPort),
		Gossip:         storage.GossipConfig{BindAddress: fmt.Sprintf("127.0.0.1:%d", gossipPort), InitialMembers: []string{fmt.Sprintf("127.0.0.1:%d", gossipPort)}},
		Table:          storage.TableConfig{FS: wrapFS(fs), TableCacheSize: 1024, ElectionRTT: 10, HeartbeatRTT: 1},
		Meta:           storage.MetaConfig{ElectionRTT: 10, HeartbeatRTT: 1},
		FS:             fs,
	}
}

func newTestEngine(t *testing.T) *storage.Engine {
	e, err := storage.New(newTestConfig(t))
	require.NoError(t, err)
	require.NoError(t, e.Start())
	require.NoError(t, e.WaitUntilReady(context.Background()))
	t.Cleanup(func() {
		defer func() { recover() }()
		require.NoError(t, e.Close())
	})
	return e
}

func startBackupServer(manager *storage.Engine) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	l, _ := net.Listen("tcp", testNodeAddress)
	server := regattaserver.NewServer(l, zap.NewNop().Sugar())
	regattapb.RegisterClusterServer(server, &regattaserver.ClusterServer{Cluster: manager})
	regattapb.RegisterMaintenanceServer(server, &regattaserver.BackupServer{AuthFunc: func(ctx context.Context) (context.Context, error) {
		return ctx, nil
	}, Tables: manager})
	go func() {
		err := server.Serve()
		if err != nil {
			panic(err)
		}
	}()
	// Let the server start.
	time.Sleep(100 * time.Millisecond)
	return server
}

func getTestPort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() {
		_ = l.Close()
	}()
	return l.Addr().(*net.TCPAddr).Port
}

// wrapFS creates a new pebble/vfs.FS instance.
func wrapFS(fs lvfs.FS) pvfs.FS {
	return &pebbleFSAdapter{fs}
}

// pebbleFSAdapter is a wrapper struct that implements the pebble/vfs.FS interface.
type pebbleFSAdapter struct {
	fs lvfs.FS
}

func (p *pebbleFSAdapter) OpenReadWrite(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// GetDiskUsage ...
func (p *pebbleFSAdapter) GetDiskUsage(path string) (pvfs.DiskUsage, error) {
	du, err := p.fs.GetDiskUsage(path)
	return pvfs.DiskUsage{
		AvailBytes: du.AvailBytes,
		TotalBytes: du.TotalBytes,
		UsedBytes:  du.UsedBytes,
	}, err
}

// Create ...
func (p *pebbleFSAdapter) Create(name string) (pvfs.File, error) {
	return p.fs.Create(name)
}

// Link ...
func (p *pebbleFSAdapter) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

// Open ...
func (p *pebbleFSAdapter) Open(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenDir ...
func (p *pebbleFSAdapter) OpenDir(name string) (pvfs.File, error) {
	return p.fs.OpenDir(name)
}

// Remove ...
func (p *pebbleFSAdapter) Remove(name string) error {
	return p.fs.Remove(name)
}

// RemoveAll ...
func (p *pebbleFSAdapter) RemoveAll(name string) error {
	return p.fs.RemoveAll(name)
}

// Rename ...
func (p *pebbleFSAdapter) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// ReuseForWrite ...
func (p *pebbleFSAdapter) ReuseForWrite(oldname, newname string) (pvfs.File, error) {
	return p.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll ...
func (p *pebbleFSAdapter) MkdirAll(dir string, perm os.FileMode) error {
	return p.fs.MkdirAll(dir, perm)
}

// Lock ...
func (p *pebbleFSAdapter) Lock(name string) (io.Closer, error) {
	return p.fs.Lock(name)
}

// List ...
func (p *pebbleFSAdapter) List(dir string) ([]string, error) {
	return p.fs.List(dir)
}

// Stat ...
func (p *pebbleFSAdapter) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

// PathBase ...
func (p *pebbleFSAdapter) PathBase(path string) string {
	return p.fs.PathBase(path)
}

// PathJoin ...
func (p *pebbleFSAdapter) PathJoin(elem ...string) string {
	return p.fs.PathJoin(elem...)
}

// PathDir ...
func (p *pebbleFSAdapter) PathDir(path string) string {
	return p.fs.PathDir(path)
}
