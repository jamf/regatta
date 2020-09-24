package main

import (
	"log"
	"net/http"

	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
)

const (
	hostname     = "localhost"
	port         = 8443
	certFilename = "hack/server.crt"
	keyFilename  = "hack/server.key"
)

func main() {
	// Create storage
	var storage storage.SimpleStorage
	storage.Reset()
	storage.PutDummyData()

	// Create regatta server
	regatta := regattaserver.NewServer(hostname, port, certFilename, keyFilename)

	// Create and register grpc/rest endpoints
	kvs := &regattaserver.KVServer{
		Storage: &storage,
	}
	if err := kvs.Register(regatta); err != nil {
		log.Fatalf("registerKVServer failed: %v", err)
	}
	ms := &regattaserver.MaintenanceServer{
		Storage: &storage,
	}
	if err := ms.Register(regatta); err != nil {
		log.Fatalf("registerMaintenanceServer failed: %v", err)
	}

	// Start serving
	if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("ListenAndServe failed: %v", err)
	}
}
