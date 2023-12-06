// Copyright JAMF Software, LLC

//go:build linux

package cmd

import (
	"fmt"

	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

func autoSetMaxprocs(log *zap.SugaredLogger) error {
	_, err := maxprocs.Set(maxprocs.Logger(log.Infof))
	if err != nil {
		return fmt.Errorf("maxprocs: failed to set GOMAXPROCS: %w", err)
	}
	return nil
}
