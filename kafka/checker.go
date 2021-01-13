package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Checker checks if kafka and topics configured in cfg are ready.
type Checker struct {
	cfg     Config
	timeout time.Duration
	log     *zap.SugaredLogger
}

// NewChecker creates checker with cfg config.
func NewChecker(cfg Config, timeout time.Duration) *Checker {
	c := Checker{}
	c.cfg = cfg
	c.timeout = timeout
	c.log = zap.S().Named("checker")
	return &c
}

// Check runs check of configured kafka and topics.
func (c *Checker) Check() bool {
	for _, t := range c.cfg.Topics {
		if !c.checkTopic(c.cfg.Brokers, t.Name) {
			return false
		}
	}
	return true
}

// dial creates connection to one of kafka brokers. It fails when no broker is reachable.
func (c *Checker) dial(brokers []string) (*kafka.Conn, error) {
	var conn *kafka.Conn
	var err error
	for _, b := range brokers {
		conn, err = kafka.Dial("tcp", b)
		if err == nil {
			return conn, nil
		}
		c.log.Errorf("unable to dial: %v", err.Error())
	}
	return nil, err
}

// checkTopic checks if topic is ready, i.e. it has at least one partition.
func (c *Checker) checkTopic(brokers []string, name string) bool {
	conn, err := c.dial(brokers)
	if err != nil {
		return false
	}
	defer conn.Close()
	if c.timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	p, err := conn.ReadPartitions(name)
	if err != nil {
		c.log.Errorf("unable to read partitions for topic `%s`: %v", name, err.Error())
		return false
	}

	if len(p) == 0 {
		c.log.Errorf("no partitions for topic `%s`", name)
		return false
	}
	return true
}
