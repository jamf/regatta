package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"log"

	"github.com/spf13/cobra"
	"github.com/wandera/regatta/cert"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	address string
	table   string
	index   int64
)

func init() {
	logReaderCmd.PersistentFlags().StringVar(&address, "logreader.leader-address", ":8444", "Address of the leader replication API to connect to.")
	logReaderCmd.PersistentFlags().StringVar(&table, "logreader.table", "", "Table to read log from.")
	logReaderCmd.PersistentFlags().Int64Var(&index, "logreader.leader-index", 0, "Follower's leader index")
}

var logReaderCmd = &cobra.Command{
	Use:    "log reader",
	Short:  "Read log from regatta leader",
	Hidden: true,
	Run:    logReader,
}

func logReader(_ *cobra.Command, _ []string) {
	logger := rl.NewLogger(true, "debug")
	defer func() {
		_ = logger.Sync()
	}()
	zap.ReplaceGlobals(logger)

	watcher := &cert.Watcher{
		CertFile: "hack/replication/client.crt",
		KeyFile:  "hack/replication/client.key",
		Log:      logger.Named("cert").Sugar(),
	}
	err := watcher.Watch()
	if err != nil {
		log.Panicf("cannot watch certificate: %v", err)
	}
	defer watcher.Stop()

	caBytes, err := ioutil.ReadFile("hack/replication/ca.crt")
	if err != nil {
		log.Panicf("cannot load server CA: %v", err)
	}
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(caBytes)

	conn, err := createConnection(cp, watcher)
	if err != nil {
		log.Panicf("could not create connection: %v", err)
	}

	client := proto.NewLogClient(conn)
	req := proto.ReplicateRequest{LeaderIndex: uint64(index), Table: []byte(table)}
	ctx := context.TODO()
	c, err := client.Replicate(ctx, &req)
	if err != nil {
		log.Panicf("failed to create client %v", err)
	}

	for {
		res, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicf("Recieved error: %v", err)
		}

		switch res := res.Response.(type) {
		case *proto.ReplicateResponse_CommandsResponse:
			for _, v := range res.CommandsResponse.Commands {
				log.Printf("%+v\n", v)
			}
		case *proto.ReplicateResponse_ErrorResponse:
			log.Printf("Error response: %s", res.ErrorResponse.Error.String())
		default:
			log.Panicf("Response is neither CommandResponse and ErrorResponse")
		}
	}
}

func createConnection(cp *x509.CertPool, replicationWatcher *cert.Watcher) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{
		RootCAs: cp,
		GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return replicationWatcher.GetCertificate(), nil
		},
	})

	replConn, err := grpc.Dial(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return replConn, nil
}
