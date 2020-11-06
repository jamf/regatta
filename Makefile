# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: proto check test build

run: build
	./regatta --dev-mode --api.reflection-api --raft.address=127.0.0.1:5012 --raft.initial-members='1=127.0.0.1:5012'

run-client: proto
	go run client/main.go

# Run golangci-lint on the code
check: proto
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.32.0
endif
	golangci-lint run

test:
	go test ./... -coverprofile cover.out -race

build: regatta

regatta: proto *.go **/*.go
	CGO_ENABLED=1 go build -o regatta

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

kind: docker-build kind-cluster
	kubectl --context kind-regatta create namespace regatta || true
	cd helm/regatta && kubecrt charts-kind.yaml | kubectl --context kind-regatta apply -f -

kind-cluster:
	kind create cluster --config hack/kind-cluster-config.yaml || true
	kind load docker-image --name regatta regatta:latest

clean:
	rm -f regatta
