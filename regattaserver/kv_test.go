package regattaserver

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
)

var kv KVServer

var (
	table1Name           = []byte("table_1")
	table2Name           = []byte("table_2")
	key1Name             = []byte("key_1")
	key2Name             = []byte("key_2")
	table1Value1         = []byte("table_1/value_1")
	table1Value1Modified = []byte("table_1/value_1_modified")
	table1Value2         = []byte("table_1/value_2")
	table2Value1         = []byte("table_2/value_1")
	table2Value2         = []byte("table_2/value_2")
)

func setup() {
	s := storage.SimpleStorage{}
	s.Reset()

	kv = KVServer{
		Storage: &s,
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

func TestRegatta_Put(t *testing.T) {
	tests := []struct {
		name           string
		putRequests    []*proto.PutRequest
		rangeRequests  []*proto.RangeRequest
		expectedValues []*proto.RangeResponse
	}{
		{
			name: "Put new kv",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
				},
			},
		},
		{
			name: "Rewrite existing kv",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1Modified,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1Modified,
						},
					},
					Count: 1,
				},
			},
		},
		{
			name: "Put more kvs",
			putRequests: []*proto.PutRequest{
				{
					Table: table1Name,
					Key:   key1Name,
					Value: table1Value1,
				},
				{
					Table: table1Name,
					Key:   key2Name,
					Value: table1Value2,
				},
				{
					Table: table2Name,
					Key:   key1Name,
					Value: table2Value1,
				},
				{
					Table: table2Name,
					Key:   key2Name,
					Value: table2Value2,
				},
			},
			rangeRequests: []*proto.RangeRequest{
				{
					Table: table1Name,
					Key:   key1Name,
				},
				{
					Table: table1Name,
					Key:   key2Name,
				},
				{
					Table: table2Name,
					Key:   key1Name,
				},
				{
					Table: table2Name,
					Key:   key2Name,
				},
			},
			expectedValues: []*proto.RangeResponse{
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table1Value1,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key2Name,
							Value: table1Value2,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key1Name,
							Value: table2Value1,
						},
					},
					Count: 1,
				},
				{
					Kvs: []*proto.KeyValue{
						{
							Key:   key2Name,
							Value: table2Value2,
						},
					},
					Count: 1,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			kv.Storage.Reset()

			t.Logf("Put kvs")
			for _, preq := range test.putRequests {
				_, err := kv.Put(context.Background(), preq)
				require.NoError(err, "Failed to put kv")
			}

			t.Logf("Get kvs")
			for i, rreq := range test.rangeRequests {
				rresp, err := kv.Range(context.Background(), rreq)
				require.NoError(err, "Failed to get value")
				require.Equal(test.expectedValues[i], rresp)
			}

		})
	}
}
