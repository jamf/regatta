package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

type topicConsumerMetrics struct {
	topic  string
	reader Reader

	kafkaReaderDialCount      *prometheus.CounterVec
	kafkaReaderFetchCount     *prometheus.CounterVec
	kafkaReaderMessageCount   *prometheus.CounterVec
	kafkaReaderBytesCount     *prometheus.CounterVec
	kafkaReaderRebalanceCount *prometheus.CounterVec
	kafkaReaderTimeoutCount   *prometheus.CounterVec
	kafkaReaderErrorCount     *prometheus.CounterVec

	kafkaReaderOffset         *prometheus.GaugeVec
	kafkaReaderLag            *prometheus.GaugeVec
	kafkaReaderMinBytes       *prometheus.GaugeVec
	kafkaReaderMaxBytes       *prometheus.GaugeVec
	kafkaReaderMaxWaitSeconds *prometheus.GaugeVec
	kafkaReaderQueueLength    *prometheus.GaugeVec
	kafkaReaderQueueCapacity  *prometheus.GaugeVec
}

func newTopicConsumerMetrics(topic string, reader Reader) *topicConsumerMetrics {
	return &topicConsumerMetrics{
		topic:  topic,
		reader: reader,
		kafkaReaderDialCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_dial_count",
			Help: "Kafka reader dials",
		}, []string{"topic"}),
		kafkaReaderFetchCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_fetch_count",
			Help: "Kafka reader fetches",
		}, []string{"topic"}),
		kafkaReaderMessageCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_message_count",
			Help: "Kafka reader messages",
		}, []string{"topic"}),
		kafkaReaderBytesCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_message_bytes",
			Help: "Kafka reader bytes",
		}, []string{"topic"}),
		kafkaReaderRebalanceCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_rebalance_count",
			Help: "Kafka reader rebalances",
		}, []string{"topic"}),
		kafkaReaderTimeoutCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_timeout_count",
			Help: "Kafka reader timeouts",
		}, []string{"topic"}),
		kafkaReaderErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_reader_error_count",
			Help: "Kafka reader errors",
		}, []string{"topic"}),

		kafkaReaderOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_offset",
			Help: "Kafka reader offset",
		}, []string{"topic"}),
		kafkaReaderLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_lag",
			Help: "Kafka reader lag",
		}, []string{"topic"}),
		kafkaReaderMinBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_min_bytes",
			Help: "Kafka reader min bytes",
		}, []string{"topic"}),
		kafkaReaderMaxBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_max_bytes",
			Help: "Kafka reader max bytes",
		}, []string{"topic"}),
		kafkaReaderMaxWaitSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_max_wait_seconds",
			Help: "Kafka reader max wait seconds",
		}, []string{"topic"}),
		kafkaReaderQueueLength: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_queue_length",
			Help: "Kafka reader queue length",
		}, []string{"topic"}),
		kafkaReaderQueueCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_reader_queue_capacity",
			Help: "Kafka reader queue capacity",
		}, []string{"topic"}),
	}
}

func (c *topicConsumerMetrics) update() {
	stats := c.reader.Stats()
	c.kafkaReaderDialCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Dials))
	c.kafkaReaderFetchCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Fetches))
	c.kafkaReaderMessageCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Messages))
	c.kafkaReaderBytesCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Bytes))
	c.kafkaReaderRebalanceCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Rebalances))
	c.kafkaReaderTimeoutCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Timeouts))
	c.kafkaReaderErrorCount.With(prometheus.Labels{"topic": c.topic}).Add(float64(stats.Errors))

	c.kafkaReaderOffset.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.Offset))
	c.kafkaReaderLag.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.Lag))
	c.kafkaReaderMinBytes.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.MinBytes))
	c.kafkaReaderMaxBytes.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.MaxBytes))
	c.kafkaReaderMaxWaitSeconds.With(prometheus.Labels{"topic": c.topic}).Set(stats.MaxWait.Seconds())
	c.kafkaReaderQueueLength.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.QueueLength))
	c.kafkaReaderQueueCapacity.With(prometheus.Labels{"topic": c.topic}).Set(float64(stats.QueueCapacity))
}

func (c *topicConsumerMetrics) Describe(ch chan<- *prometheus.Desc) {
	c.kafkaReaderDialCount.Describe(ch)
	c.kafkaReaderFetchCount.Describe(ch)
	c.kafkaReaderMessageCount.Describe(ch)
	c.kafkaReaderBytesCount.Describe(ch)
	c.kafkaReaderRebalanceCount.Describe(ch)
	c.kafkaReaderTimeoutCount.Describe(ch)
	c.kafkaReaderErrorCount.Describe(ch)
	c.kafkaReaderOffset.Describe(ch)
	c.kafkaReaderLag.Describe(ch)
	c.kafkaReaderMinBytes.Describe(ch)
	c.kafkaReaderMaxBytes.Describe(ch)
	c.kafkaReaderMaxWaitSeconds.Describe(ch)
	c.kafkaReaderQueueLength.Describe(ch)
	c.kafkaReaderQueueCapacity.Describe(ch)
}

func (c *topicConsumerMetrics) Collect(ch chan<- prometheus.Metric) {
	c.update()
	c.kafkaReaderDialCount.Collect(ch)
	c.kafkaReaderFetchCount.Collect(ch)
	c.kafkaReaderMessageCount.Collect(ch)
	c.kafkaReaderBytesCount.Collect(ch)
	c.kafkaReaderRebalanceCount.Collect(ch)
	c.kafkaReaderTimeoutCount.Collect(ch)
	c.kafkaReaderErrorCount.Collect(ch)
	c.kafkaReaderOffset.Collect(ch)
	c.kafkaReaderLag.Collect(ch)
	c.kafkaReaderMinBytes.Collect(ch)
	c.kafkaReaderMaxBytes.Collect(ch)
	c.kafkaReaderMaxWaitSeconds.Collect(ch)
	c.kafkaReaderQueueLength.Collect(ch)
	c.kafkaReaderQueueCapacity.Collect(ch)
}
