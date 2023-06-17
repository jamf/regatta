// Copyright JAMF Software, LLC

package snapshot

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	pb "google.golang.org/protobuf/proto"
)

func Test_snapshotFile_Read(t *testing.T) {
	r := require.New(t)

	sf, err := OpenFile("testdata/snapshot.bin")
	r.NoError(err)
	defer func() {
		_ = sf.Close()
	}()

	buff := make([]byte, 1024)
	for {
		n, err := sf.Read(buff)
		if n > 0 {
			r.NoError(err)
			r.NoError(pb.Unmarshal(buff[:n], &proto.Command{}))
		} else {
			r.Equal(io.EOF, err)
			break
		}
	}
}

func Test_snapshotFile_Write(t *testing.T) {
	r := require.New(t)
	sf, err := NewTemp()
	r.NoError(err)
	defer func() {
		_ = sf.Close()
	}()

	bts, _ := pb.Marshal(&proto.Command{Type: proto.Command_PUT, Kv: &proto.KeyValue{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	}})
	n, err := sf.Write(bts)
	r.NoError(err)
	r.NoError(sf.Sync())
	r.Equal(len(bts), n)
	r.FileExists(sf.Path())
}

func Test_snapshotFile_ReadWrite(t *testing.T) {
	const testString = "barbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbar"
	r := require.New(t)
	sf, err := NewTemp()
	r.NoError(err)
	defer func() {
		_ = sf.Close()
	}()
	for i := 0; i < 1000; i++ {
		bts, _ := pb.Marshal(&proto.Command{Type: proto.Command_PUT, Kv: &proto.KeyValue{
			Key:   []byte("foo" + strconv.Itoa(i)),
			Value: []byte(testString),
		}})
		n, err := sf.Write(bts)
		r.NoError(err)
		r.Equal(len(bts), n)
	}
	r.NoError(sf.Sync())

	sf.Seek(0, 0)
	buff := make([]byte, 1024)
	for {
		n, err := sf.Read(buff)
		if n > 0 {
			r.NoError(err)
			m := &proto.Command{}
			r.NoError(pb.Unmarshal(buff[:n], m))
			r.Equal(m.Kv.Value, []byte(testString))
		} else {
			r.Equal(io.EOF, err)
			break
		}
	}
}

func TestReaderWriter(t *testing.T) {
	lis := bufconn.Listen(10 * 1024 * 1024)
	srv := grpc.NewServer()
	proto.RegisterSnapshotServer(srv, &mockSnapshotServer{cmds: []*proto.Command{
		{
			Table: []byte("table"),
			Type:  proto.Command_PUT,
			Kv:    &proto.KeyValue{Key: []byte("key"), Value: []byte(util.RandString(1024))},
		},
		{
			Table: []byte("table"),
			Type:  proto.Command_PUT,
			Kv:    &proto.KeyValue{Key: []byte("key2"), Value: []byte(util.RandString(1024))},
		},
		{
			Table: []byte("table"),
			Type:  proto.Command_PUT,
			Kv:    &proto.KeyValue{Key: []byte("key3"), Value: []byte(util.RandString(1024))},
		},
	}})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.DialContext(context.Background(), "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	sc := proto.NewSnapshotClient(conn)
	s, err := sc.Stream(context.Background(), &proto.SnapshotRequest{})
	require.NoError(t, err)

	_, err = io.Copy(io.Discard, &Reader{Stream: s})
	require.NoError(t, err)

	s, err = sc.Stream(context.Background(), &proto.SnapshotRequest{})
	require.NoError(t, err)

	r := &Reader{Stream: s, Limiter: rate.NewLimiter(rate.Limit(256), int(256))}
	for {
		b := make([]byte, 512)
		_, err := r.Read(b)
		if err == io.EOF {
			break
		}
	}
	require.NoError(t, err)
}

type mockSnapshotServer struct {
	proto.UnimplementedSnapshotServer
	cmds []*proto.Command
}

func (m *mockSnapshotServer) Stream(req *proto.SnapshotRequest, srv proto.Snapshot_StreamServer) error {
	sf, err := NewTemp()
	if err != nil {
		return err
	}
	defer func() {
		_ = sf.Close()
		_ = os.Remove(sf.Path())
	}()
	for _, cmd := range m.cmds {
		d, _ := cmd.MarshalVT()
		_, _ = sf.Write(d)
	}
	if err != nil {
		return err
	}
	// Write dummy command with leader index to commit recovery snapshot.
	final, err := (&proto.Command{
		Table:       req.Table,
		Type:        proto.Command_DUMMY,
		LeaderIndex: m.cmds[len(m.cmds)-1].LeaderIndex,
	}).MarshalVT()
	if err != nil {
		return err
	}
	_, err = sf.Write(final)
	if err != nil {
		return err
	}
	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(&Writer{Sender: srv}, bufio.NewReaderSize(sf.File, DefaultSnapshotChunkSize))
	return err
}
