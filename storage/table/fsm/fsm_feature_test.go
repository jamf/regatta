// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table/key"
	"go.uber.org/zap"
)

/*
This file is used for generating test data (`TestGenerateData`) to be provided
to the state machine via commands and checking whether the state machine
stays consistent (`TestDataConsistency`). The purpose of these tests is to
catch undesired modifications to the state machine when refactoring or adding
new features.

When adding a feature to the state machine, resulting in a new command
available, add the new commands to the `input` map as a new key-value
pair with the version one higher than the highest version in the map and the commands
as a slice `*proto.Command`s. Before running the tests, remove the skipping
of tests in `TestGenerateData` to generate the data for the new commands.
Before committing, put the line back in.
*/

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
	1: {
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
				Key:   []byte("not_match"),
				Value: []byte("value"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_DELETE,
			Kv: &proto.KeyValue{
				Key: []byte("key"),
			},
			RangeEnd: incrementRightmostByte([]byte("key")),
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
			Type:  proto.Command_PUT,
			Kv: &proto.KeyValue{
				Key:   key.LatestMaxKey,
				Value: []byte("value_3"),
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_DELETE,
			Kv: &proto.KeyValue{
				Key: []byte{0},
			},
			RangeEnd: []byte{0},
		},
	},
	2: {
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_2"),
							Value: []byte("value"),
						}},
					},
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_3"),
							Value: []byte("value"),
						}},
					},
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_4"),
							Value: []byte("value"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key_1")}},
				Failure: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("value"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key_1")}},
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("valuevaluevalue"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key_1"), Result: proto.Compare_EQUAL, Target: proto.Compare_VALUE, TargetUnion: &proto.Compare_Value{Value: []byte("valuevaluevalue")}}},
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("value1"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key_1"), Result: proto.Compare_LESS, Target: proto.Compare_VALUE, TargetUnion: &proto.Compare_Value{Value: []byte("value")}}},
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("value2"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key_1"), Result: proto.Compare_GREATER, Target: proto.Compare_VALUE, TargetUnion: &proto.Compare_Value{Value: []byte("value")}}},
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("value"),
						}},
					},
				},
				Failure: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_1"),
							Value: []byte("value2"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestRange{RequestRange: &proto.RequestOp_Range{
							Key: []byte("key_1"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("key"), RangeEnd: wildcard, Result: proto.Compare_GREATER, Target: proto.Compare_VALUE, TargetUnion: &proto.Compare_Value{Value: []byte("val")}}},
				Success: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_5"),
							Value: []byte("value"),
						}},
					},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_TXN,
			Txn: &proto.Txn{
				Compare: []*proto.Compare{{Key: []byte("nonsense"), RangeEnd: []byte("nonsense2")}},
				Failure: []*proto.RequestOp{
					{
						Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
							Key:   []byte("key_6"),
							Value: []byte("value"),
						}},
					},
				},
			},
		},
	},
	3: {
		{
			Table: []byte("test"),
			Type:  proto.Command_SEQUENCE,
			Sequence: []*proto.Command{
				{
					Table: []byte("test"),
					Type:  proto.Command_TXN,
					Txn: &proto.Txn{
						Success: []*proto.RequestOp{
							{
								Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
									Key:   []byte("key_1"),
									Value: []byte("value"),
								}},
							},
							{
								Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
									Key:   []byte("key_2"),
									Value: []byte("value"),
								}},
							},
							{
								Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{
									Key:   []byte("key_3"),
									Value: []byte("value"),
								}},
							},
						},
					},
				},
				{
					Table: []byte("test"),
					Type:  proto.Command_DELETE,
					Kv:    &proto.KeyValue{Key: []byte("key_2")},
				},
				{
					Table: []byte("test"),
					Type:  proto.Command_DELETE,
					Kv:    &proto.KeyValue{Key: []byte("key_3")},
				},
				{
					Table: []byte("test"),
					Type:  proto.Command_PUT,
					Kv:    &proto.KeyValue{Key: []byte("key_1"), Value: []byte("value_1")},
				},
			},
		},
		{
			Table: []byte("test"),
			Type:  proto.Command_SEQUENCE,
			Sequence: []*proto.Command{
				{
					Table: []byte("test"),
					Type:  proto.Command_PUT,
					Kv:    &proto.KeyValue{Key: []byte("key_2"), Value: []byte("value_2")},
				},
				{
					Table: []byte("test"),
					Type:  proto.Command_PUT,
					Kv:    &proto.KeyValue{Key: []byte("key_3"), Value: []byte("value_3")},
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
func generateFiles(t *testing.T, version int, inputCommands []*proto.Command) {
	inFile, err := os.Create(path.Join("testdata", fmt.Sprintf("v%d-input.json", version)))
	if err != nil {
		t.Fatal(err)
	}
	defer inFile.Close()

	outFile, err := os.Create(path.Join("testdata", fmt.Sprintf("v%d-output.json", version)))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	fsm, err := createTestFSM()
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()
	db := fsm.pebble.Load()

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
	defer iter.Close()
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
	fsm := &FSM{
		fs:        vfs.NewMem(),
		clusterID: 1,
		nodeID:    1,
		tableName: "test",
		dirname:   "/tmp",
		closed:    false,
		log:       zap.NewNop().Sugar(),
		metrics:   newMetrics("test", 1),
	}

	db, err := rp.OpenDB(fsm.dirname, rp.WithFS(fsm.fs))
	if err != nil {
		return nil, err
	}
	fsm.pebble.Store(db)
	return fsm, err
}

func TestDataConsistency(t *testing.T) {
	for version := 0; version < len(input); version++ {
		testConsistency(t, version)
	}
}

func testConsistency(t *testing.T, version int) {
	r := require.New(t)

	inFile, err := os.Open(path.Join("testdata", fmt.Sprintf("v%d-input.json", version)))
	if err != nil {
		r.NoError(err)
	}
	defer inFile.Close()

	outFile, err := os.Open(path.Join("testdata", fmt.Sprintf("v%d-output.json", version)))
	if err != nil {
		r.NoError(err)
	}
	defer outFile.Close()

	fsm, err := createTestFSM()
	if err != nil {
		r.NoError(err)
	}
	defer fsm.Close()
	db := fsm.pebble.Load()

	var inputRecords []inputRecord
	r.NoError(json.NewDecoder(inFile).Decode(&inputRecords))

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
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r.Equal(outputRecords[i].Key, iter.Key())
		r.Equal(outputRecords[i].Value, iter.Value())
		r.NoError(iter.Error())
		i++
	}
	r.Equal(len(outputRecords), i)
	r.NoError(iter.Close())
}
