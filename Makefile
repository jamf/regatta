# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: proto check test build

prepare:
	@echo "Downloading tools"
ifeq (, $(shell which go-junit-report))
	go get github.com/jstemmer/go-junit-report
endif
ifeq (, $(shell which gocov))
	go get github.com/axw/gocov/gocov
endif
ifeq (, $(shell which gocov-xml))
	go get github.com/AlekSi/gocov-xml
endif

run: build
	./regatta leader --dev-mode --api.reflection-api --raft.address=127.0.0.1:5012 --raft.initial-members='1=127.0.0.1:5012' --tables.names=regatta-test

run-follower: build
	./regatta follower --dev-mode --api.reflection-api --raft.address=127.0.0.1:6012 --raft.initial-members='1=127.0.0.1:6012' --api.address=:9443 --rest.address=:8080 --replication.leader-address=127.0.0.1:8444 --raft.node-host-dir=/tmp/regatta-follower/raft --raft.state-machine-dir=/tmp/regatta-follower/state-machine

run-client: proto
	go run client/main.go

# Run golangci-lint on the code
check: proto
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.41.0
endif
	golangci-lint run

test: prepare
	mkdir -p report
	go test ./... -coverprofile report/coverage.txt -race -v > report/report.txt 2>&1
	go-junit-report -set-exit-code < report/report.txt > report/report.xml
	gocov convert report/coverage.txt | gocov-xml > report/coverage.xml

build: regatta docs

docs: regatta
	./regatta docs

regatta: proto *.go **/*.go
	CGO_ENABLED=1 go build -o regatta

proto: proto/mvcc.pb.go proto/regatta.pb.go proto/regatta_grpc.pb.go proto/replication.pb.go proto/replication_grpc.pb.go

proto/mvcc.pb.go proto/regatta.pb.go proto/regatta_grpc.pb.go proto/replication.pb.go proto/replication_grpc.pb.go: proto/*.proto
	protoc -I proto/ --go_out=. --go-grpc_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool --go-vtproto_opt=pool=./proto.Command --go-vtproto_opt=pool=./proto.SnapshotChunk proto/*.proto

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
