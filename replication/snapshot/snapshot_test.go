package snapshot

import (
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

func Test_snapshotFile_Read(t *testing.T) {
	r := require.New(t)

	sf, err := NewFile("testdata/snapshot.bin")
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
	r.Equal(len(bts)+8, n)
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
		r.Equal(len(bts)+8, n)
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
