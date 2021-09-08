package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/replication/backup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	backupCmd.PersistentFlags().String("address", "127.0.0.1:8444", "Backup API address")
	backupCmd.PersistentFlags().String("dir", "", "Target dir (current directory if empty)")
	backupCmd.PersistentFlags().String("cert", "hack/replication/client.crt", "Path to the client certificate.")
	backupCmd.PersistentFlags().String("key", "hack/replication/client.key", "Path to the client private key file.")
	backupCmd.PersistentFlags().String("ca", "hack/replication/ca.crt", "Path to the client CA cert file.")
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup regatta to local files",
	Long: `Command backs up regatta into a directory of choice, it currently back up all the tables present in the target server.
Backup consist of file per a table in binary compressed form + human-readable manifest file. Use restore command to load backup into the server.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		caBytes, err := ioutil.ReadFile(viper.GetString("ca"))
		if err != nil {
			return err
		}
		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(caBytes)

		creds := credentials.NewTLS(&tls.Config{
			RootCAs: cp,
			GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				keyPair, err := tls.LoadX509KeyPair(viper.GetString("cert"), viper.GetString("key"))
				return &keyPair, err
			},
		})
		conn, err := grpc.Dial(viper.GetString("address"), grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}

		b := backup.Backup{
			Conn: conn,
			Dir:  viper.GetString("dir"),
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
