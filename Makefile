# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: proto check test build

run: build
	./regatta --dev-mode --reflection-api --raft-address=127.0.0.1:5012

run-client: proto
	go run client/main.go

# Run golangci-lint on the code
check: proto
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.30.0
endif
	golangci-lint run

test:
	go test ./... -coverprofile cover.out -race

build: regatta

regatta: proto *.go **/*.go
	CGO_ENABLED=0 go build -o regatta

proto: proto/regatta.pb.go proto/regatta.pb.gw.go proto/mvcc.pb.go

proto/regatta.pb.go: proto/regatta.proto
	protoc -I proto/ --go_out=plugins=grpc,paths=source_relative:./proto proto/regatta.proto

proto/regatta.pb.gw.go: proto/regatta.proto
	protoc -I proto/ --grpc-gateway_out=logtostderr=true,paths=source_relative:./proto proto/regatta.proto

proto/mvcc.pb.go: proto/mvcc.proto
	protoc -I proto/ --go_out=paths=source_relative:./proto proto/mvcc.proto

# Build the docker image
docker-build: proto
	docker build . -t ${IMG}

clean:
	rm -f regatta
