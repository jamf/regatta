package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	addr          string
	certFilename  string
	keyFilename   string
	logLevel      string
	reflectionAPI bool
)

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:8443", "addr to listen on")
	rootCmd.PersistentFlags().StringVar(&certFilename, "cert-filename", "hack/server.crt", "path to the certificate")
	rootCmd.PersistentFlags().StringVar(&keyFilename, "key-filename", "hack/server.key", "path to the certificate key file")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "DEBUG", "log level: DEBUG/INFO/WARN/ERROR")
	rootCmd.PersistentFlags().BoolVar(&reflectionAPI, "reflection-api", false, "whether reflection API is provided. Should not be turned on in production.")
}

var rootCmd = &cobra.Command{
	Use:   "regatta",
	Short: "Regatta is read-optimized distributed key-value store.",
	Run: func(cmd *cobra.Command, args []string) {
		logCfg := zap.NewProductionConfig()
		logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		var level zapcore.Level
		if err := level.Set(logLevel); err != nil {
			panic(err)
		}
		logCfg.Level.SetLevel(level)
		logger, err := logCfg.Build()
		if err != nil {
			zap.S().Fatal("Failed to build logger: %v", err)
		}
		defer logger.Sync()
		zap.ReplaceGlobals(logger)

		// Create storage
		var storage storage.SimpleStorage
		storage.Reset()
		storage.PutDummyData()

		// Create regatta server
		regatta := regattaserver.NewServer(addr, certFilename, keyFilename, reflectionAPI)

		// Create and register grpc/rest endpoints
		kvs := &regattaserver.KVServer{
			Storage: &storage,
		}
		if err := kvs.Register(regatta); err != nil {
			zap.S().Fatalf("registerKVServer failed: %v", err)
		}
		ms := &regattaserver.MaintenanceServer{
			Storage: &storage,
		}
		if err := ms.Register(regatta); err != nil {
			zap.S().Fatalf("registerMaintenanceServer failed: %v", err)
		}

		// Start serving
		if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
			zap.S().Fatalf("ListenAndServe failed: %v", err)
		}
	},
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
