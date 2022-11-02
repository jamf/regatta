// Copyright JAMF Software, LLC

package kafka

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ReaderMock mocks Reader from kafka-go.
type ReaderMock struct {
	// number of fetches done since reset
	fetchCount int
	// number of commits done since reset
	commitCount int
	// whether fetches fail
	failFetch bool
	// whether commits fail
	failCommit bool
	// whether the reader is closed
	closed bool
	// messages which can be fetched with `FetchMessage`
	messages []kafka.Message
	mtx      sync.RWMutex
	log      *zap.SugaredLogger
}

// NewTopicConsumerMock constructs TopicConsumer with ReaderMock.
func NewTopicConsumerMock(brokers []string, dialer *kafka.Dialer, config TopicConfig, listener OnMessageFunc, debugLogs bool) *TopicConsumer {
	tc := TopicConsumer{}

	tc.config = config
	tc.listener = listener
	tc.log = zap.S().Named(fmt.Sprintf("consumer:%s", config.Name))
	tc.reader = &ReaderMock{log: tc.log}
	tc.metrics = newTopicConsumerMetrics(config.Name, config.GroupID, tc.reader)
	return &tc
}

// SetFetchResponses sets massages returned in FetchMessage.
func (r *ReaderMock) SetFetchResponses(messages []kafka.Message) {
	r.mtx.Lock()
	r.messages = messages
	r.mtx.Unlock()
}

// SetFailFetch sets whether FetchMessage will fail.
func (r *ReaderMock) SetFailFetch(failFetch bool) {
	r.mtx.Lock()
	r.failFetch = failFetch
	r.mtx.Unlock()
}

// SetFailCommit sets whether CommitMessages will fail.
func (r *ReaderMock) SetFailCommit(failCommit bool) {
	r.mtx.Lock()
	r.failCommit = failCommit
	r.mtx.Unlock()
}

// GetFetchCount gets `fetchCount`.
func (r *ReaderMock) GetFetchCount() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return r.fetchCount
}

// IncrementFetchCount increments `fetchCount`.
func (r *ReaderMock) IncrementFetchCount() {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.fetchCount++
}

// GetCommitCount gets `commitCount`.
func (r *ReaderMock) GetCommitCount() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return r.commitCount
}

// IncrementCommitCount increments `commitCount`.
func (r *ReaderMock) IncrementCommitCount() {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.commitCount++
}

// FetchMessage reads messages from mock.
// If reader is set to `closed` it returns `io.EOF`.
// If reader is set to `failFetch` it returns error.
// If there is a message to fetch it is returned.
// Otherwise it blocks.
func (r *ReaderMock) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if ctx.Err() != nil {
		return kafka.Message{}, context.Canceled
	}
	r.mtx.RLock()
	if r.closed {
		r.log.Infof("Reader closed")
		r.mtx.RUnlock()
		return kafka.Message{}, io.EOF
	}
	if r.failFetch {
		r.mtx.RUnlock()
		return kafka.Message{}, fmt.Errorf("Failed to fetch")
	}
	r.mtx.RUnlock()
	var m kafka.Message
	if r.GetFetchCount() < len(r.messages) {
		m = r.messages[r.GetFetchCount()]
		r.IncrementFetchCount()
	} else {
		for {
			r.mtx.RLock()
			if r.closed {
				r.mtx.RUnlock()
				return kafka.Message{}, context.Canceled
			}
			r.mtx.RUnlock()
			time.Sleep(time.Second)
		}
	}
	return m, nil
}

// CommitMessages mocks kafka message commiting.
func (r *ReaderMock) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.mtx.RLock()
	if r.closed || ctx.Err() != nil {
		r.log.Infof("Reader closed")
		r.mtx.RUnlock()
		return context.Canceled
	}
	if r.failCommit {
		r.mtx.RUnlock()
		return fmt.Errorf("Failed to commit")
	}
	r.mtx.RUnlock()

	r.IncrementCommitCount()
	return nil
}

// Config mocks getting reader config.
func (r *ReaderMock) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{}
}

// Stats get the reader stats.
func (r *ReaderMock) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{
		Dials:                     1,
		Fetches:                   2,
		Messages:                  3,
		Bytes:                     4,
		Rebalances:                5,
		Timeouts:                  6,
		Errors:                    7,
		DialTime:                  kafka.DurationStats{},
		ReadTime:                  kafka.DurationStats{},
		WaitTime:                  kafka.DurationStats{},
		FetchSize:                 kafka.SummaryStats{},
		FetchBytes:                kafka.SummaryStats{},
		Offset:                    8,
		Lag:                       9,
		MinBytes:                  10,
		MaxBytes:                  11,
		MaxWait:                   12 * time.Second,
		QueueLength:               13,
		QueueCapacity:             14,
		ClientID:                  "",
		Topic:                     "mock-topic",
		Partition:                 "",
		DeprecatedFetchesWithTypo: 0,
	}
}

// Close closes mock.
func (r *ReaderMock) Close() error {
	r.mtx.Lock()
	r.closed = true
	r.mtx.Unlock()
	return nil
}

// Reset resets mock.
func (r *ReaderMock) Reset() {
	r.mtx.Lock()
	r.fetchCount = 0
	r.commitCount = 0
	r.mtx.Unlock()
}

// ListenerMock is mock for OnMessageFunc.
type ListenerMock struct {
	// whether OnMessageFunc fails
	fail bool
	// number of writes done since reset
	processedCount int
	mtx            sync.RWMutex
}

// OnMessage mocks OnMessageFunc.
func (l *ListenerMock) OnMessage(ctx context.Context, table, key, value []byte) error {
	log := zap.S().Named("listener")
	l.mtx.RLock()
	if l.fail {
		l.mtx.RUnlock()
		log.Errorf("Failed to write table: %s key: %s value: %s", table, key, value)
		return fmt.Errorf("Failed to write table: %s key: %s value: %s", table, key, value)
	}
	l.mtx.RUnlock()
	l.IncrementProcessedCount()
	log.Infof("Write table: %s key: %s value: %s", table, key, value)
	return nil
}

// SetFail sets whether `OnMessage` will fail.
func (l *ListenerMock) SetFail(fail bool) {
	l.mtx.Lock()
	l.fail = fail
	l.mtx.Unlock()
}

// GetProcessedCount gets `processedCount`.
func (l *ListenerMock) GetProcessedCount() int {
	l.mtx.RLock()
	defer l.mtx.RUnlock()
	return l.processedCount
}

// IncrementProcessedCount increments `processedCount`.
func (l *ListenerMock) IncrementProcessedCount() {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.processedCount++
}

// Reset resets mock.
func (l *ListenerMock) Reset() {
	l.mtx.Lock()
	l.processedCount = 0
	l.mtx.Unlock()
}
