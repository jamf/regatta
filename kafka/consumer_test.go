package kafka

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	msgs   []kafka.Message
	topics []TopicConfig
)

func setup() {
	zap.ReplaceGlobals(zap.NewNop())

	msgs = []kafka.Message{
		{
			Topic:     "topic1",
			Partition: 0,
			Offset:    0,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
		},
		{
			Topic:     "topic1",
			Partition: 0,
			Offset:    0,
			Key:       []byte("key2"),
			Value:     []byte("value2"),
		},
		{
			Topic:     "topic1",
			Partition: 0,
			Offset:    0,
			Key:       []byte("key3"),
			Value:     []byte("value3"),
		},
	}

	topics = []TopicConfig{
		{
			Name:    "topic1",
			GroupID: "test",
			Table:   "table1",
		},
		{
			Name:    "topic2",
			GroupID: "test",
			Table:   "table2",
		},
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

func TestKafka_CreateConsumer(t *testing.T) {
	r := require.New(t)

	cfg := Config{
		Brokers: []string{"localhost:9092"},
	}

	consumer, err := NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")
	r.EqualValues(0, len(consumer.topicConsumers), "No topic consumers should exist")

	cfg.TLS = true

	consumer, err = NewConsumer(cfg)
	r.EqualError(err, "open : no such file or directory", "Should return error")

	cfg.ClientCertFilename = "testdata/test.crt"
	cfg.ClientKeyFilename = "testdata/test.key"

	consumer, err = NewConsumer(cfg)
	r.EqualError(err, "open : no such file or directory", "Should return error")

	cfg.ServerCertFilename = "testdata/test.crt"

	consumer, err = NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")

	cfg.Topics = topics
	consumer, err = NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")
	r.EqualValues(2, len(consumer.topicConsumers), "Two topic consumers should exist")

	for i, tc := range consumer.topicConsumers {
		r.EqualValues(cfg.Topics[i], tc.config, "Topic config should be set")
		r.NotNil(tc.reader, "Reader should be created")
		r.EqualValues(cfg.Brokers, tc.reader.Config().Brokers, "Reader config should be set correctly: brokers")
		r.EqualValues(cfg.Topics[i].Name, tc.reader.Config().Topic, "Reader config should be set correctly: topic")
		r.EqualValues(cfg.Topics[i].GroupID, tc.reader.Config().GroupID, "Reader config should be set correctly: groupID")
		r.NotNil(tc.reader.Config().Dialer, "Dialer should be set")
		r.Nil(tc.reader.Config().Logger, "Logger should not be set")
		r.NotNil(tc.reader.Config().ErrorLogger, "Error logger should be set")
	}
}

func TestKafka_StartConsumer(t *testing.T) {
	r := require.New(t)

	cfg := Config{
		Brokers:   []string{"localhost:9092"},
		DebugLogs: true,
	}

	consumer, err := NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")

	consumer.Start(context.Background())
	r.NotNil(consumer.cancel, "Cancel function should be set")

	cfg.Topics = topics
	consumer, err = NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")

	err = consumer.Start(context.Background())
	r.NoError(err, "Failed to start consumer")

	consumer.Close()
}

func TestKafka_StartTopicConsumer(t *testing.T) {
	r := require.New(t)

	tc := TopicConsumer{}
	var wg sync.WaitGroup
	wg.Add(1)
	err := tc.Start(context.Background(), &wg)
	wg.Wait()

	r.EqualError(err, "failed to start not initialized topic consumer", "Error expected")
	err = tc.Close()
	r.NoError(err, "No error expected")
}

func TestKafka_CloseConsumer(t *testing.T) {
	r := require.New(t)

	cfg := Config{
		Brokers: []string{"localhost:9092"},
	}

	consumer, err := NewConsumer(cfg)
	r.NoError(err, "Failed to create consumer")
	consumer.Close()
}

func TestKafka_ConsumeTopic(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)
	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(context.Background(), &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return len(msgs) == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be fetched",
	)

	a.Eventually(
		func() bool { return len(msgs) == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be written",
	)
	a.Eventually(
		func() bool { return len(msgs) == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be commited",
	)
}

func TestKafka_ConsumeTopicFailToFetch(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)
	mock.SetFailFetch(true)

	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(context.Background(), &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return 0 == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "No fetch should occur",
	)
	a.Eventually(
		func() bool { return 0 == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "No write should occur",
	)
	a.Eventually(
		func() bool { return 0 == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "No commit should occur",
	)
}

func TestKafka_ConsumeTopicContextCancelled(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(ctx, &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return 0 == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "No fetch should occur",
	)
	a.Eventually(
		func() bool { return 0 == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "No write should occur",
	)
	a.Eventually(
		func() bool { return 0 == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "No commit should occur",
	)
}

func TestKafka_ConsumeTopicFailToWrite(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{fail: true}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)

	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(context.Background(), &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return 1 == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "Only one fetch should occur",
	)
	a.Eventually(
		func() bool { return 0 == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "No write should occur",
	)
	a.Eventually(
		func() bool { return 0 == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "No commit should occur",
	)
}

func TestKafka_ConsumeTopicRetryToWrite(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{fail: true}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)

	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(context.Background(), &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return 1 == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "Only one fetch should occur",
	)
	a.Eventually(
		func() bool { return 0 == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "No write should occur",
	)
	a.Eventually(
		func() bool { return 0 == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "No commit should occur",
	)

	w.SetFail(false)

	a.Eventually(
		func() bool { return len(msgs) == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be fetched",
	)

	a.Eventually(
		func() bool { return len(msgs) == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be written",
	)
	a.Eventually(
		func() bool { return len(msgs) == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be commited",
	)
}

func TestKafka_ConsumeTopicFailToCommit(t *testing.T) {
	a := assert.New(t)

	w := ListenerMock{}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)
	mock := tc.reader.(*ReaderMock)
	mock.SetFetchResponses(msgs)
	mock.SetFailCommit(true)

	var wg sync.WaitGroup
	wg.Add(1)
	tc.Start(context.Background(), &wg)
	wg.Wait()

	a.Eventually(
		func() bool { return 1 == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "Only one fetch should occur",
	)
	a.Eventually(
		func() bool { return 1 == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "Only one write should occur",
	)
	a.Eventually(
		func() bool { return 0 == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "No commit should occur",
	)

	mock.SetFailCommit(false)

	a.Eventually(
		func() bool { return len(msgs) == mock.GetFetchCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be fetched",
	)

	a.Eventually(
		func() bool { return len(msgs) == w.GetProcessedCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be written",
	)
	a.Eventually(
		func() bool { return len(msgs) == mock.GetCommitCount() },
		1*time.Second, 10*time.Millisecond, "All messages should be commited",
	)
}

func TestKafka_Metrics(t *testing.T) {
	r := require.New(t)

	w := ListenerMock{}
	tc := NewTopicConsumerMock([]string{"address"},
		kafka.DefaultDialer,
		topics[0],
		w.OnMessage,
		true,
	)

	metrics, err := os.Open("testdata/metrics")
	r.NoError(err)

	err = testutil.CollectAndCompare(tc, metrics)
	r.NoError(err)
}
