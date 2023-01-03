// Copyright JAMF Software, LLC

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/replication/backup"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	restoreCmd.PersistentFlags().String("address", "127.0.0.1:8445", "Maintenance API address.")
	restoreCmd.PersistentFlags().String("dir", "", "Directory containing the backups (current directory if empty)")
	restoreCmd.PersistentFlags().String("ca", "", "Path to the client CA cert file.")
	restoreCmd.PersistentFlags().String("token", "", "The access token to use for the authentication.")
	restoreCmd.PersistentFlags().Bool("json", false, "Enables JSON logging.")
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore Regatta from local files.",
	Long: `WARNING: Restoring from backup is a destructive operation and should be used only as part of break glass procedure.

Restore Regatta cluster from a directory of choice. All tables present in the manifest.json will be restored.
Restoring is done sequentially, for the fine-grained control of what to restore use backup manifest file.
It is almost certain that after restore the cold-start of all the followers watching the restored leader cluster is going to be necessary.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cp *x509.CertPool
		ca := viper.GetString("ca")
		if ca != "" {
			caBytes, err := os.ReadFile(ca)
			if err != nil {
				return err
			}
			cp = x509.NewCertPool()
			cp.AppendCertsFromPEM(caBytes)
		}

		creds := credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    cp,
		})
		conn, err := grpc.Dial(viper.GetString("address"), grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(tokenCredentials(viper.GetString("token"))))
		if err != nil {
			return err
		}

		b := backup.Backup{
			Conn: conn,
			Dir:  viper.GetString("dir"),
		}
		if viper.GetBool("json") {
			l := rl.NewLogger(false, zap.InfoLevel.String())
			b.Log = l.Sugar()
		}
		return b.Restore()
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return nil
	},
	DisableAutoGenTag: true,
}
