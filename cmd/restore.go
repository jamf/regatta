package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/replication/backup"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	restoreCmd.PersistentFlags().String("address", "127.0.0.1:8445", "Regatta maintenance API address")
	restoreCmd.PersistentFlags().String("dir", "", "Target dir (current directory if empty)")
	restoreCmd.PersistentFlags().String("ca", "", "Path to the client CA cert file.")
	restoreCmd.PersistentFlags().String("token", "", "The access token to use for the authentication.")
	restoreCmd.PersistentFlags().Bool("json", false, "Enables JSON logging.")
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore regatta from local files",
	Long: `WARNING: The restore is a destructive operation and should be used only as part of break glass procedure.
Command restore regatta cluster from a directory of choice, it will restore all the tables present in the manifest.json.
The restore will be done sequentially, for the fine-grained control of what to restore use backup manifest file.
It is almost certain that after restore the cold-start of all the followers watching the cluster restored is going to be necessary.`,
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

		creds := credentials.NewTLS(&tls.Config{RootCAs: cp})
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
