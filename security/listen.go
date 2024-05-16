// Copyright JAMF Software, LLC

package security

import (
	"net"
	"sync/atomic"

	"github.com/jamf/regatta/util"
	"go.uber.org/zap"
)

func NewTrackingListener(l net.Listener) *TrackingListener {
	return &TrackingListener{
		Listener: l,
		stats:    util.NewSyncMap[net.Addr, *atomic.Uint64](nil),
	}
}

type trackingConn struct {
	net.Conn
	l *TrackingListener
}

func (c *trackingConn) Close() error {
	defer func() {
		curr, ok := c.l.stats.Load(c.RemoteAddr())
		if ok {
			curr.Add(^uint64(0))
			if curr.Load() == 0 {
				c.l.stats.Delete(c.RemoteAddr())
			}
		}
	}()
	return c.Conn.Close()
}

type TrackingListener struct {
	net.Listener
	stats *util.SyncMap[net.Addr, *atomic.Uint64]
}

func (tl *TrackingListener) Accept() (net.Conn, error) {
	c, err := tl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := &trackingConn{Conn: c, l: tl}
	curr := tl.stats.ComputeIfAbsent(conn.RemoteAddr(), func(addr net.Addr) *atomic.Uint64 { return &atomic.Uint64{} })
	curr.Add(1)
	return conn, nil
}

func (tl *TrackingListener) Stats() map[net.Addr]uint64 {
	res := make(map[net.Addr]uint64)
	tl.stats.Pairs()(func(s util.SyncMapPair[net.Addr, *atomic.Uint64]) bool {
		res[s.Key] = s.Val.Load()
		return true
	})
	return res
}

func NewLoggingListener(l net.Listener, log *zap.SugaredLogger) *LoggingListener {
	return &LoggingListener{
		Listener: l,
		log:      log,
	}
}

type loggingConn struct {
	net.Conn
	log *zap.SugaredLogger
}

func (c *loggingConn) Close() error {
	defer func() {
		c.log.Infof("%s connection closed", c.RemoteAddr())
	}()
	return c.Conn.Close()
}

type LoggingListener struct {
	net.Listener
	log *zap.SugaredLogger
}

func (l *LoggingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.log.Infof("%s connection open", c.RemoteAddr())
	return &loggingConn{Conn: c, log: l.log}, nil
}
