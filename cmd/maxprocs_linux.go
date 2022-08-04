//go:build linux

package cmd

import (
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

func autoSetMaxprocs(log *zap.SugaredLogger) {
	_, err := maxprocs.Set(maxprocs.Logger(log.Infof))
	if err != nil {
		log.Fatalf("maxprocs: failed to set GOMAXPROCS: %v", err)
	}
}
