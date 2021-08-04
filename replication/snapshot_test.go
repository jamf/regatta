package replication

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	pb "google.golang.org/protobuf/proto"
)

func Test_snapshotFile_Read(t *testing.T) {
	r := require.New(t)
	sf := snapshotFile{}
	open, err := os.Open("testdata/snapshot.bin")
	r.NoError(err)
	sf.File = open
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
	sf, err := newSnapshotFile(t.TempDir(), snapshotFilenamePattern)
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
	r.Equal(len(bts)+8, n)
}

func startSnapshotServer(manager *tables.Manager) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	proto.RegisterSnapshotServer(server, &regattaserver.SnapshotServer{Tables: manager})
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	return server
}
