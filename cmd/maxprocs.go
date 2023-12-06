// Copyright JAMF Software, LLC

//go:build !linux

package cmd

import (
	"runtime"

	"go.uber.org/zap"
)

func autoSetMaxprocs(log *zap.SugaredLogger) error {
	log.Infof("maxprocs: unsupported system for auto-setting GOMAXPROCS using value GOMAXPROCS=%d", runtime.GOMAXPROCS(0))
	return nil
}
