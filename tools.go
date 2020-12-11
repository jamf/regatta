// +build tools

package tools

import (
	_ "github.com/axw/gocov/gocov"
	_ "github.com/jstemmer/go-junit-report"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
)
