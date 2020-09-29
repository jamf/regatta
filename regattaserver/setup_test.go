package regattaserver

import (
	"os"
	"testing"

	"github.com/wandera/regatta/storage"
)

func setup() {
	s := storage.SimpleStorage{}
	s.Reset()

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
