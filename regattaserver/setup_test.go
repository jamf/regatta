package regattaserver

import (
	"context"
	"os"
	"testing"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
)

func setup() {
	s := storage.Mock{}
	_ = s.Reset(context.TODO(), &proto.ResetRequest{})

	kv = KVServer{
		Storage: &s,
	}

	ms = MaintenanceServer{
		Storage: &s,
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}
