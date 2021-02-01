package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// OnMessageFunc is function called for every fetched kafka message.
type OnMessageFunc func(ctx context.Context, table, key, value []byte) error

// Consumer is kafka consumer which can consume one or more topics and processes
// fetched messages with `WriterFunc`.
type Consumer struct {
	dialer         *kafka.Dialer
	config         Config
	topicConsumers []*TopicConsumer
	listener       OnMessageFunc
	cancel         context.CancelFunc
	log            *zap.SugaredLogger
}

// NewConsumer constructs Consumer with `config` and `listener`.
func NewConsumer(config Config, listener OnMessageFunc) (*Consumer, error) {
	c := new(Consumer)
	c.config = config
	c.log = zap.S().Named("consumer")

	var clCert tls.Certificate
	var caCert []byte
	var caCertPool *x509.CertPool
	var tlsConf *tls.Config
	var err error

	if c.config.TLS {
		clCert, err = tls.LoadX509KeyPair(c.config.ClientCertFilename, c.config.ClientKeyFilename)
		if err != nil {
			return nil, err
		}

		caCert, err = ioutil.ReadFile(c.config.ServerCertFilename)
		if err != nil {
			return nil, err
		}
		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConf = &tls.Config{
			Certificates:          []tls.Certificate{clCert},
			RootCAs:               caCertPool,
			InsecureSkipVerify:    true, // Not actually skipping, the cert is checked using VerifyPeerCertificate
			VerifyPeerCertificate: verifyPeerCertificate(caCertPool),
		}
	}
	c.dialer = &kafka.Dialer{
		Timeout:   c.config.DialerTimeout,
		DualStack: true,
		TLS:       tlsConf,
	}

	c.listener = listener

	c.topicConsumers = make([]*TopicConsumer, 0, len(c.config.Topics))
	for _, tc := range c.config.Topics {
		c.topicConsumers = append(c.topicConsumers,
			NewTopicConsumer(c.config.Brokers, c.dialer, tc, c.listener, c.config.DebugLogs))
	}
	return c, nil
}

// Start starts Consumer, i.e. starts to consume configured topics and processes the messages.
func (c *Consumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	var wg sync.WaitGroup
	wg.Add(len(c.topicConsumers))

	for _, tc := range c.topicConsumers {
		c.log.Infof("starting topic consumer: %s", tc.config.Name)
		if err := tc.Start(ctx, &wg); err != nil {
			c.log.Panicf("failed to start topic consumer: %s. Error: %v", tc.config.Name, err)
		}
	}
	wg.Wait()
	return nil
}

// Close closes all topic consumers.
func (c *Consumer) Close() {
	if c.cancel != nil {
		c.cancel()
		for _, tc := range c.topicConsumers {
			_ = tc.Close()
		}
	}
}

func (c *Consumer) Collect(ch chan<- prometheus.Metric) {
	for _, tc := range c.topicConsumers {
		tc.Collect(ch)
	}
}

func (c *Consumer) Describe(ch chan<- *prometheus.Desc) {
	for _, tc := range c.topicConsumers {
		tc.Describe(ch)
	}
}

// TopicConsumer reads one topic from Kafka and processes the messages with `listener`.
type TopicConsumer struct {
	config   TopicConfig
	reader   Reader
	listener OnMessageFunc
	log      *zap.SugaredLogger
	metrics  *topicConsumerMetrics
}

// NewTopicConsumer constructs TopicConsumer.
func NewTopicConsumer(brokers []string, dialer *kafka.Dialer, config TopicConfig, listener OnMessageFunc, debugLogs bool) *TopicConsumer {
	tc := TopicConsumer{}
	tc.config = config
	tc.log = zap.S().Named(fmt.Sprintf("consumer:%s", config.Name))

	rc := kafka.ReaderConfig{
		Brokers:               brokers,
		GroupID:               config.GroupID,
		Topic:                 config.Name,
		Dialer:                dialer,
		QueueCapacity:         1000,
		MaxBytes:              10e6, // 10MB
		MaxWait:               3 * time.Second,
		CommitInterval:        1 * time.Second,
		RetentionTime:         7 * 24 * time.Hour,
		WatchPartitionChanges: true,
		ErrorLogger:           kafka.LoggerFunc(tc.log.Errorf),
	}
	tc.listener = listener
	if debugLogs {
		rc.Logger = kafka.LoggerFunc(tc.log.Debugf)
	}
	tc.reader = kafka.NewReader(rc)
	tc.metrics = newTopicConsumerMetrics(config.Name, tc.reader)
	return &tc
}

// Start checks reader is initialized and starts consuming kafka topic and processing the fetched messages. It is non-blocking.
// Consumption can be stopped by cancelling the `ctx` or by calling `Close()` method.
func (tc *TopicConsumer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	if tc.reader == nil {
		wg.Done()
		return fmt.Errorf("failed to start not initialized topic consumer")
	}

	go tc.Consume(ctx, wg)

	return nil
}

// Consume starts consuming kafka topic and processing the fetched messages. It is blocking.
// Consumption can be stopped by cancelling the `ctx` or by calling `Close()` method.
func (tc *TopicConsumer) Consume(ctx context.Context, wg *sync.WaitGroup) {
	table := []byte(tc.config.Table)
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0 // infinite
	wg.Done()
	for {
		select {
		case <-ctx.Done():
			tc.log.Infof("stop reading topic: %s, reader has been closed", tc.config.Name)
			return
		default:
		}

		m, err := tc.reader.FetchMessage(ctx)
		if err != nil {
			tc.log.Error(err)
			continue
		}

		b.Reset()
		err = backoff.Retry(func() error {
			if err := ctx.Err(); err != nil {
				tc.log.Errorf("context closed: %v", err)
				return backoff.Permanent(err)
			}

			if err := tc.listener(ctx, table, m.Key, m.Value); err != nil {
				tc.log.Errorf("failed to write message from topic: %v to storage: %v", m.Topic, err)
				return err
			}
			return nil
		}, b)
		if err != nil {
			tc.log.Errorf("failed to retry: %v", err)
			break
		}

		b.Reset()
		err = backoff.Retry(func() error {
			if err := ctx.Err(); err != nil {
				tc.log.Errorf("context closed: %v", err)
				return backoff.Permanent(err)
			}

			if err := tc.reader.CommitMessages(ctx, m); err != nil {
				tc.log.Errorf("failed to commmit message from topic: %v: %v", m.Topic, err)
				return err
			}
			return nil
		}, b)
		if err != nil {
			tc.log.Errorf("failed to retry: %v", err)
			break
		}
	}
}

// Close stops topic consumer.
func (tc *TopicConsumer) Close() error {
	if tc.reader != nil {
		tc.log.Infof("topic reader for topic: %s closed", tc.config.Name)
		return tc.reader.Close()
	}
	return nil
}

func (tc *TopicConsumer) Collect(ch chan<- prometheus.Metric) {
	tc.metrics.Collect(ch)
}

func (tc *TopicConsumer) Describe(ch chan<- *prometheus.Desc) {
	tc.metrics.Describe(ch)
}

// verifyPeerCertificate is used for verification of certificates with wrong hostname, i.e. current wandera situation.
// Inspired by https://go-review.googlesource.com/c/go/+/193620/8/src/crypto/tls/example_test.go.
func verifyPeerCertificate(rootCAs *x509.CertPool) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(certificates [][]byte, _ [][]*x509.Certificate) error {
		certs := make([]*x509.Certificate, len(certificates))
		for i, asn1Data := range certificates {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return errors.New("tls: failed to parse certificate from server: " + err.Error())
			}
			certs[i] = cert
		}
		opts := x509.VerifyOptions{
			Roots:         rootCAs,
			DNSName:       "", // skip hostname verification
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(opts)
		return err
	}
}

// Reader is interface for readers implementing reading from Kafka.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Config() kafka.ReaderConfig
	Stats() kafka.ReaderStats
	Close() error
}
