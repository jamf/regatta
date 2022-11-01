# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: proto check test build

run: build
	./regatta leader --dev-mode --api.reflection-api --raft.address=127.0.0.1:5012 --raft.initial-members='1=127.0.0.1:5012' --tables.names=regatta-test

run-follower: build
	./regatta follower --dev-mode --api.reflection-api --raft.address=127.0.0.1:6012 --raft.initial-members='1=127.0.0.1:6012' --api.address=:9443 --maintenance.address=:9445 --rest.address=:8080 --replication.leader-address=127.0.0.1:8444 --raft.node-host-dir=/tmp/regatta-follower/raft --raft.state-machine-dir=/tmp/regatta-follower/state-machine

# Run golangci-lint on the code
check: proto
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ${GOPATH}/bin v1.49.0
endif
	golangci-lint run

test:
	go test ./... -cover -race -v

build: regatta docs

docs: regatta
	./regatta docs --destination=docs/cli

regatta: proto *.go **/*.go
	CGO_ENABLED=1 go build -o regatta

PROTO_GO_OUTS=proto/mvcc.pb.go proto/mvcc_vtproto.pb.go \
 proto/regatta.pb.go proto/regatta_grpc.pb.go proto/regatta_vtproto.pb.go \
 proto/replication.pb.go proto/replication_grpc.pb.go proto/replication_vtproto.pb.go \
 proto/maintenance.pb.go proto/maintenance_grpc.pb.go proto/maintenance_vtproto.pb.go

proto: $(PROTO_GO_OUTS)

$(PROTO_GO_OUTS): proto/*.proto
	protoc -I proto/ --go_out=. --go-grpc_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool --go-vtproto_opt=pool=./proto.Command --go-vtproto_opt=pool=./proto.SnapshotChunk proto/*.proto --doc_out=./docs --doc_opt=markdown,api.md

# Build the docker image
docker-build: proto
	docker build . -t ${IMG}

kind: docker-build kind-cluster
	kubectl --context kind-regatta create namespace regatta || true
	cd helm/regatta && kubecrt charts-kind.yaml | kubectl --context kind-regatta apply -f -
	kubectl -n regatta rollout restart statefulset/regatta

kind-cluster:
	kind create cluster --config hack/kind-cluster-config.yaml || true
	kind load docker-image --name regatta regatta:latest

clean:
	rm -f regatta

