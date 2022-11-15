// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jamf/regatta/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	generatorCmd.PersistentFlags().String("address", "127.0.0.1:8443", "Address of the leader write API to connect to.")
	generatorCmd.PersistentFlags().String("table", "regatta-test", "Table to write to.")
	generatorCmd.PersistentFlags().Uint64("entries", 1_000, "Amount of entries to generate.")
}

var generatorCmd = &cobra.Command{
	Use:    "generator",
	Short:  "Generate data in Regatta",
	Hidden: true,
	Run:    generator,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return nil
	},
}

func generator(_ *cobra.Command, _ []string) {
	address := viper.GetViper().GetString("address")
	table := viper.GetViper().GetString("table")
	entries := viper.GetViper().GetUint64("entries")

	log.Printf("address: '%s', table: '%s', entries to generate: %d\n", address, table, entries)

	creds, err := credentials.NewClientTLSFromFile("hack/server.crt", "")
	if err != nil {
		log.Fatalf("Cannot create credentials: %v", err)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("could not establish connection: %v", err)
	}

	client := proto.NewKVClient(conn)
	ctx := context.TODO()

	started := time.Now().Unix()
	req := proto.PutRequest{Table: []byte(table)}
	ok := 0
	for i := uint64(0); i < entries; i++ {
		req.Key = []byte(fmt.Sprintf("%d_%d", started, i))
		req.Value = []byte(fmt.Sprintf("value_%d", i))

		if _, err := client.Put(ctx, &req); err != nil {
			log.Printf("could not write %s/%s: %v", req.Key, req.Value, err)
		} else {
			ok++
		}
	}

	log.Printf("OK, generated %d/%d k/v pairs, key prefix is '%d_'", ok, entries, started)
}
