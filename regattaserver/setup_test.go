package regattaserver

import (
	"context"
	"os"
	"testing"

	"github.com/wandera/regatta/proto"
)

func setup() {
	s := MockStorage{}
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
