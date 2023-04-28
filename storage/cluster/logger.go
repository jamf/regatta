// Copyright JAMF Software, LLC

package cluster

import (
	"regexp"
	"strings"

	"go.uber.org/zap"
)

const (
	logRegexpDate  = `(?P<date>[0-9]{4}/[0-9]{2}/[0-9]{2})?[ ]?`
	logRegexpTime  = `(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?)?[ ]?`
	logRegexpFile  = `(?P<file>[^: ]+?:[0-9]+)?(: )?`
	logRegexpLevel = `(\[(?P<level>[A-Z]+)\] )?`
	logRegexpMsg   = `(?:memberlist: )?(?P<msg>.*)`

	lineRegexp = logRegexpDate + logRegexpTime + logRegexpFile + logRegexpLevel + logRegexpMsg
)

var logRegexp = regexp.MustCompile(lineRegexp)

type loggerAdapter struct {
	log *zap.SugaredLogger
}

func (l *loggerAdapter) Write(p []byte) (n int, err error) {
	result := parseLogLine(p)

	logFn := l.log.Debug
	if lvl, ok := result["level"]; ok {
		switch strings.ToLower(lvl) {
		case "debug":
			logFn = l.log.Debug
		case "warn":
			logFn = l.log.Warn
		case "info":
			logFn = l.log.Info
		case "err", "error":
			logFn = l.log.Error
		}
	}
	if msg, ok := result["msg"]; ok {
		logFn(msg)
	}
	return len(p), nil
}

func parseLogLine(line []byte) map[string]string {
	m := logRegexp.FindSubmatch(line)
	if len(m) < len(logRegexp.SubexpNames()) {
		return make(map[string]string)
	}
	result := make(map[string]string)
	for i, name := range logRegexp.SubexpNames() {
		if name != "" {
			result[name] = string(m[i])
		}
	}
	return result
}
