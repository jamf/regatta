package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/replication/backup"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	backupCmd.PersistentFlags().String("address", "127.0.0.1:8445", "Regatta maintenance API address")
	backupCmd.PersistentFlags().String("dir", "", "Target dir (current directory if empty)")
	backupCmd.PersistentFlags().String("ca", "", "Path to the client CA cert file.")
	backupCmd.PersistentFlags().String("token", "", "The access token to use for the authentication.")
	backupCmd.PersistentFlags().Bool("json", false, "Enables JSON logging.")
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup regatta to local files",
	Long: `Command backs up regatta into a directory of choice, it currently backs up all the tables present in the target server.
Backup consist of file per a table in binary compressed form + human-readable manifest file. Use restore command to load backup into the server.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cp *x509.CertPool
		ca := viper.GetString("ca")
		if ca != "" {
			caBytes, err := ioutil.ReadFile(ca)
			if err != nil {
				return err
			}
			cp = x509.NewCertPool()
			cp.AppendCertsFromPEM(caBytes)
		}

		creds := credentials.NewTLS(&tls.Config{RootCAs: cp})
		conn, err := grpc.Dial(viper.GetString("address"), grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(token(viper.GetString("token"))))
		if err != nil {
			return err
		}

		b := backup.Backup{
			Conn: conn,
			Dir:  viper.GetString("dir"),
		}
		if viper.GetBool("json") {
			l, err := zap.NewProduction()
			if err != nil {
				return err
			}
			b.Log = l.Sugar()
		}
		_, err = b.Backup()
		return err
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return nil
	},
	DisableAutoGenTag: true,
}
