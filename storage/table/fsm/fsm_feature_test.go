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
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap/zaptest"
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
	for version, commands := range input {
		generateFiles(t, version, commands)
	}
}

func generateFiles(t *testing.T, index int, inputCommands []*proto.Command) {
	inputFile := fmt.Sprintf("v%d-input.json", index)
	inFile, err := os.Create(path.Join("testdata", inputFile))
	if err != nil {
		t.Fatal(err)
	}
	defer inFile.Close()

	outputFile := fmt.Sprintf("v%d-output.json", index)
	outFile, err := os.Create(path.Join("testdata", outputFile))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	c := pebble.NewCache(0)
	defer c.Unref()
	fsm := FSM{
		pebble:     nil,
		wo:         &pebble.WriteOptions{Sync: true},
		fs:         vfs.NewMem(),
		clusterID:  1,
		nodeID:     1,
		tableName:  "test",
		dirname:    "/tmp",
		walDirname: "/tmp",
		closed:     false,
		log:        zaptest.NewLogger(t).Sugar(),
		blockCache: c,
	}

	db, err := rp.OpenDB(fsm.fs, fsm.dirname, fsm.walDirname, fsm.blockCache)
	if err != nil {
		t.Fatal(err)
	}
	atomic.StorePointer(&fsm.pebble, unsafe.Pointer(db))

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
		outputs = append(outputs, outputRecord{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
	}

	oe := json.NewEncoder(outFile)
	oe.SetIndent("", "  ")
	if err := oe.Encode(outputs); err != nil {
		t.Fatal(err)
	}
}
