package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"log"
	"net/http"
	"os"
)

var (
	addr         string
	certFilename string
	keyFilename  string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:8443", "addr to listen on")
	rootCmd.PersistentFlags().StringVar(&certFilename, "cert-filename", "hack/server.crt", "path to the certificate")
	rootCmd.PersistentFlags().StringVar(&keyFilename, "key-filename", "hack/server.key", "path to the certificate key file")
}

var rootCmd = &cobra.Command{
	Use:   "regatta",
	Short: "Regatta is read-optimized distributed key-value store.",
	Run: func(cmd *cobra.Command, args []string) {
		// Create storage
		var storage storage.SimpleStorage
		storage.Reset()
		storage.PutDummyData()

		// Create regatta server
		regatta := regattaserver.NewServer(addr, certFilename, keyFilename)

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
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
