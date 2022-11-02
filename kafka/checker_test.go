// Copyright JAMF Software, LLC

package kafka

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	unreachable = "unreachable-kafka:9902"
	mock        = "mock"
)

func TestKafka_Dial(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		wantErr bool
	}{
		{
			name:    "one unreachable",
			brokers: []string{unreachable},
			wantErr: true,
		},
		{
			name:    "two unreachable",
			brokers: []string{unreachable, unreachable},
			wantErr: true,
		},
		{
			name:    "first unreachable",
			brokers: []string{unreachable, mock},
			wantErr: false,
		},
		{
			name:    "second unreachable",
			brokers: []string{mock, unreachable},
			wantErr: false,
		},
	}
	ms := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ms.Close()
	url := strings.TrimPrefix(ms.URL, "http://")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{}
			for _, b := range test.brokers {
				if b == "mock" {
					cfg.Brokers = append(cfg.Brokers, url)
				} else {
					cfg.Brokers = append(cfg.Brokers, b)
				}
			}
			ch := NewChecker(cfg, 1*time.Second)
			con, err := ch.dial(cfg.Brokers)
			if test.wantErr {
				assert.Error(t, err)
				assert.Nil(t, con)
			} else {
				assert.NotNil(t, con)
			}
		})
	}
}

func TestKafka_Check(t *testing.T) {
	tests := []struct {
		name             string
		topics           []string
		unreachableKafka bool
		check            bool
	}{
		{
			name:             "unreachable kafka",
			topics:           []string{"topic1"},
			unreachableKafka: true,
			check:            false,
		},
		{
			name:             "unable to read partitions",
			topics:           []string{"topic1"},
			unreachableKafka: false,
			check:            false,
		},
		{
			name:             "no topics",
			unreachableKafka: false,
			check:            true,
		},
	}
	ms := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ms.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			url := strings.TrimPrefix(ms.URL, "http://")
			if test.unreachableKafka {
				url = unreachable
			}
			cfg := Config{
				Brokers: []string{url},
			}
			for _, topic := range test.topics {
				cfg.Topics = append(cfg.Topics, TopicConfig{Name: topic})
			}
			ch := NewChecker(cfg, 1*time.Second)
			assert.Equal(t, test.check, ch.Check())
		})
	}
}
