package raft

import (
	"github.com/lni/dragonboat/v3/logger"
	"go.uber.org/zap"
)

// NewLogger builds a Dragonboat compatible named zap logger.
func NewLogger(pkgName string) logger.ILogger {
	return zapLogger{z: zap.New(zap.L().Core(), zap.AddCaller(), zap.AddCallerSkip(2)).Named(pkgName).Sugar()}
}

type zapLogger struct {
	z *zap.SugaredLogger
}

func (l zapLogger) SetLevel(_ logger.LogLevel) {
	// NoOP
}

func (l zapLogger) Debugf(format string, args ...interface{}) {
	l.z.Debugf(format, args...)
}

func (l zapLogger) Infof(format string, args ...interface{}) {
	l.z.Infof(format, args...)
}

func (l zapLogger) Warningf(format string, args ...interface{}) {
	l.z.Warnf(format, args...)
}

func (l zapLogger) Errorf(format string, args ...interface{}) {
	l.z.Errorf(format, args...)
}

func (l zapLogger) Panicf(format string, args ...interface{}) {
	l.z.Panicf(format, args...)
}
