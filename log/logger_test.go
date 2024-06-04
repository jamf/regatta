// Copyright JAMF Software, LLC

package log

import (
	"testing"

	"github.com/jamf/regatta/raft/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestNewLogger(t *testing.T) {
	type args struct {
		devMode bool
		level   string
	}
	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "dev mode on known level",
			args: args{
				devMode: true,
				level:   "INFO",
			},
		},
		{
			name: "dev mode off known level",
			args: args{
				devMode: false,
				level:   "INFO",
			},
		},
		{
			name: "dev mode on unknown level",
			args: args{
				devMode: true,
				level:   "FOO",
			},
			wantPanic: true,
		},
		{
			name: "dev mode off unknown level",
			args: args{
				devMode: false,
				level:   "FOO",
			},
			wantPanic: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.wantPanic {
				require.Panics(t, func() {
					NewLogger(test.args.devMode, test.args.level)
				})
			} else {
				require.NotPanics(t, func() {
					NewLogger(test.args.devMode, test.args.level)
				})
			}
		})
	}
}

func TestLoggerFactory(t *testing.T) {
	type args struct {
		pkgName string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Some package name",
			args: args{
				pkgName: "some",
			},
		},
		{
			name: "Other package name",
			args: args{
				pkgName: "some-other",
			},
		},
		{
			name: "Long package name",
			args: args{
				pkgName: "1654847897987sd9a7sd98as4da9s84d98a7sd9a84sd98as7d9a8d456asd4a9s7f9e84f9w8e7f98we4",
			},
		},
		{
			name: "Empty package name",
			args: args{
				pkgName: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LoggerFactory(zaptest.NewLogger(t))(tt.args.pkgName)
		})
	}
}

func Test_zapLogger_SetLevel(t *testing.T) {
	type args struct {
		level logger.LogLevel
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Set level to CRITICAL",
			args: args{level: logger.CRITICAL},
		},
		{
			name: "Set level to ERROR",
			args: args{level: logger.ERROR},
		},
		{
			name: "Set level to WARNING",
			args: args{level: logger.WARNING},
		},
		{
			name: "Set level to INFO",
			args: args{level: logger.INFO},
		},
		{
			name: "Set level to DEBUG",
			args: args{level: logger.DEBUG},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LoggerFactory(zaptest.NewLogger(t))("").SetLevel(tt.args.level)
		})
	}
}

func Test_zapLogger_Debugf(t *testing.T) {
	l, _ := zap.NewDevelopment()
	log := zapLogger{l.Sugar(), logger.DEBUG}
	log.Debugf("some")
	_ = l.Sync()
}

func Test_zapLogger_Infof(t *testing.T) {
	l, _ := zap.NewDevelopment()
	log := zapLogger{l.Sugar(), logger.DEBUG}
	log.Infof("some")
	_ = l.Sync()
}

func Test_zapLogger_Warningf(t *testing.T) {
	l, _ := zap.NewDevelopment()
	log := zapLogger{l.Sugar(), logger.DEBUG}
	log.Warningf("some")
	_ = l.Sync()
}

func Test_zapLogger_Errorf(t *testing.T) {
	l, _ := zap.NewDevelopment()
	log := zapLogger{l.Sugar(), logger.DEBUG}
	log.Errorf("some")
	_ = l.Sync()
}

func Test_zapLogger_Panicf(t *testing.T) {
	l, _ := zap.NewDevelopment()
	log := zapLogger{l.Sugar(), logger.DEBUG}
	require.Panics(t, func() {
		log.Panicf("some")
	})
}
