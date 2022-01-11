package fsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
)

type inputRecord struct {
	Cmd []byte `json:"cmd"`
}

type outputRecord struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// input in form of version: commands to apply.
var input = map[int][]*proto.Command{
	0: {
		{
			Table: []byte("test"),
			Type:  proto.Command_PUT,
			Kv: &proto.KeyValue{
				Key:   []byte("key_1"),
				Value: []byte("value_1"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_PUT,
			Kv: &proto.KeyValue{
				Key:   []byte("key_2"),
				Value: []byte("value_2"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_PUT,
			Kv: &proto.KeyValue{
				Key:   []byte("key_2"),
				Value: []byte("value_2_new"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_PUT,
			Kv: &proto.KeyValue{
				Key:   []byte("key_3"),
				Value: []byte("value_3"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_DELETE,
			Kv: &proto.KeyValue{
				Key:   []byte("key_3"),
				Value: []byte("value_3"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_DELETE,
			Kv: &proto.KeyValue{
				Key:   []byte("key_3"),
				Value: []byte("value_3"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_PUT_BATCH,
			Batch: []*proto.KeyValue{
				{
					Key:   []byte("key_10"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_10"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_11"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_12"),
					Value: []byte("value"),
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_DELETE_BATCH,
			Batch: []*proto.KeyValue{
				{
					Key:   []byte("key_10"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_10"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key_11"),
					Value: []byte("value"),
				},
			},
		},
	},
}

// TestGenerateData is useful for generating test data for new features.
func TestGenerateData(t *testing.T) {
	t.Skip("Unskip for generation of a new version")
	for version, commands := range input {
		generateFiles(t, version, commands)
	}
}

//nolint:unused
func generateFiles(t *testing.T, index int, inputCommands []*proto.Command) {
	inFile, err := os.Create(path.Join("testdata", fmt.Sprintf("v%d-input.json", index)))
	if err != nil {
		t.Fatal(err)
	}
	defer inFile.Close()

	outFile, err := os.Create(path.Join("testdata", fmt.Sprintf("v%d-output.json", index)))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	fsm, err := createTestFSM()
	if err != nil {
		t.Fatal(err)
	}
	db := (*pebble.DB)(atomic.LoadPointer(&fsm.pebble))

	var inputs []inputRecord
	for i, cmd := range inputCommands {
		cmdBytes := mustMarshallProto(cmd)
		inputs = append(inputs, inputRecord{Cmd: cmdBytes})
		_, err := fsm.Update([]sm.Entry{
			{
				Index:  uint64(i),
				Cmd:    cmdBytes,
				Result: sm.Result{},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	ie := json.NewEncoder(inFile)
	ie.SetIndent("", "  ")
	if err := ie.Encode(inputs); err != nil {
		t.Fatal(err)
	}

	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	var outputs []outputRecord
	iter := db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		record := outputRecord{
			Key:   make([]byte, len(iter.Key())),
			Value: make([]byte, len(iter.Value())),
		}
		copy(record.Key, iter.Key())
		copy(record.Value, iter.Value())
		outputs = append(outputs, record)
	}

	oe := json.NewEncoder(outFile)
	oe.SetIndent("", "  ")
	if err := oe.Encode(outputs); err != nil {
		t.Fatal(err)
	}
}

func createTestFSM() (*FSM, error) {
	c := pebble.NewCache(0)
	fsm := &FSM{
		pebble:     nil,
		wo:         &pebble.WriteOptions{Sync: true},
		fs:         vfs.NewMem(),
		clusterID:  1,
		nodeID:     1,
		tableName:  "test",
		dirname:    "/tmp",
		walDirname: "/tmp",
		closed:     false,
		log:        zap.NewNop().Sugar(),
		blockCache: c,
	}

	db, err := rp.OpenDB(fsm.fs, fsm.dirname, fsm.walDirname, fsm.blockCache)
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&fsm.pebble, unsafe.Pointer(db))
	return fsm, err
}

func TestDataConsistency(t *testing.T) {
	for index := 0; index < len(input); index++ {
		testConsistency(t, index)
	}
}

func testConsistency(t *testing.T, index int) {
	r := require.New(t)

	inFile, err := os.Open(path.Join("testdata", fmt.Sprintf("v%d-input.json", index)))
	if err != nil {
		r.NoError(err)
	}
	defer inFile.Close()

	outFile, err := os.Open(path.Join("testdata", fmt.Sprintf("v%d-output.json", index)))
	if err != nil {
		r.NoError(err)
	}
	defer outFile.Close()

	fsm, err := createTestFSM()
	if err != nil {
		r.NoError(err)
	}
	defer fsm.Close()
	db := (*pebble.DB)(atomic.LoadPointer(&fsm.pebble))

	var inputRecords []inputRecord
	r.NoError(json.NewDecoder(inFile).Decode(&input))

	for i, record := range inputRecords {
		_, err := fsm.Update([]sm.Entry{
			{
				Index:  uint64(i),
				Cmd:    record.Cmd,
				Result: sm.Result{},
			},
		})
		r.NoError(err)
	}

	var outputRecords []outputRecord
	r.NoError(json.NewDecoder(outFile).Decode(&outputRecords))

	i := 0
	iter := db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		r.Equal(outputRecords[i].Key, iter.Key())
		r.Equal(outputRecords[i].Value, iter.Value())
		r.NoError(iter.Error())
		i++
	}
	r.Equal(len(outputRecords), i)
	r.NoError(iter.Close())
}
