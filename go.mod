module github.com/wandera/regatta

go 1.16

replace github.com/lni/dragonboat/v3 => github.com/coufalja/dragonboat/v3 v3.1.1-0.20210723112057-cc2022ab93b1

require (
	github.com/VictoriaMetrics/metrics v1.17.2
	github.com/axw/gocov v1.0.0
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/cockroachdb/pebble v0.0.0-20210605001512-cbda11b8689f
	github.com/fsnotify/fsnotify v1.4.9
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jstemmer/go-junit-report v0.9.1
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/dragonboat/v3 v3.1.1-0.20210605150813-01d1dcbb062c
	github.com/lni/vfs v0.2.0
	github.com/oxtoacart/bpool v0.0.0-20190530202638-03653db5a59c
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/segmentio/kafka-go v0.4.15
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.18.1
	golang.org/x/exp v0.0.0-20201008143054-e3b2a7f2fdc7 // indirect
	google.golang.org/grpc v1.38.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.27.1
)
