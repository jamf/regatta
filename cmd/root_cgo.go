// +build cgo

package cmd

import (
	"github.com/lni/dragonboat/v3/plugin/rocksdb"
)

func init() {
	logDBFactory = rocksdb.NewLogDB
}
