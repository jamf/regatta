package raft

import (
	"strings"

	"github.com/lni/dragonboat/v3/logger"
	"go.uber.org/zap"
)

// NewLogger builds a Dragonboat compatible named zap logger.
func NewLogger(pkgName string) logger.ILogger {
	return &zapLogger{z: zap.New(zap.L().Core(), zap.AddCaller(), zap.AddCallerSkip(2)).Named(pkgName).Sugar()}
}

type zapLogger struct {
	z   *zap.SugaredLogger
	lvl logger.LogLevel
}

func (l *zapLogger) SetLevel(lvl logger.LogLevel) {
	l.lvl = lvl
}

func (l *zapLogger) Debugf(format string, args ...interface{}) {
	if l.lvl >= logger.DEBUG {
		l.z.Debugf(strings.TrimRight(format, "\n"), args...)
	}
}

func (l *zapLogger) Infof(format string, args ...interface{}) {
	if l.lvl >= logger.INFO {
		l.z.Infof(strings.TrimRight(format, "\n"), args...)
	}
}

func (l *zapLogger) Warningf(format string, args ...interface{}) {
	if l.lvl >= logger.WARNING {
		l.z.Warnf(strings.TrimRight(format, "\n"), args...)
	}
}

func (l *zapLogger) Errorf(format string, args ...interface{}) {
	if l.lvl >= logger.ERROR {
		l.z.Errorf(strings.TrimRight(format, "\n"), args...)
	}
}

func (l *zapLogger) Panicf(format string, args ...interface{}) {
	l.z.Panicf(strings.TrimRight(format, "\n"), args...)
}
