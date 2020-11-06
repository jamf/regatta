// +build cgo

package cmd

import (
	"github.com/lni/dragonboat/v3/plugin/rocksdb"
	"github.com/spf13/viper"
)

func init() {
	if viper.GetBool("experimental.rocksdb") {
		logDBFactory = rocksdb.NewLogDB
	}
}
