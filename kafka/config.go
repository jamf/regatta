package kafka

import "time"

// Config for Kafka reader.
type Config struct {
	Brokers            []string
	DialerTimeout      time.Duration
	TLS                bool
	ServerCertFilename string
	ClientCertFilename string
	ClientKeyFilename  string
	Topics             []TopicConfig
	DebugLogs          bool
}

// TopicConfig for Kafka reader.
type TopicConfig struct {
	Name    string
	GroupID string
	Table   string
}
