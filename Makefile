LDFLAGS = -X github.com/jamf/regatta/cmd.Version=$(VERSION)
VERSION ?= $(shell git describe --tags --always --dirty)
CGO_ENABLED ?= 1
REPOSITORY = regatta

.PHONY: all
all: check test build

.PHONY: run
run: build
	./regatta leader --dev-mode --api.reflection-api --raft.address=127.0.0.1:5012 --raft.initial-members='1=127.0.0.1:5012' --tables.names=regatta-test

.PHONY: run-follower
run-follower: build
	./regatta follower --dev-mode --api.reflection-api --raft.address=127.0.0.1:6012 --raft.initial-members='1=127.0.0.1:6012' --api.address=:9443 --maintenance.address=:9445 --rest.address=:8080 --replication.leader-address=127.0.0.1:8444 --raft.node-host-dir=/tmp/regatta-follower/raft --raft.state-machine-dir=/tmp/regatta-follower/state-machine

# Run golangci-lint on the code
.PHONY: check
check: proto
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ${GOPATH}/bin v1.49.0
endif
	golangci-lint run

.PHONY: test
test:
	go test ./... -cover -race -v

.PHONY: build
build: *.go **/*.go proto docs regatta

docs: regatta
	./regatta docs --destination=docs/operations_guide/cli

regatta:
	test $(VERSION) || (echo "version not set"; exit 1)
	CGO_ENABLED=$(CGO_ENABLED) go build -ldflags="$(LDFLAGS)" -o regatta

PROTO_GO_OUTS=proto/mvcc.pb.go proto/mvcc_vtproto.pb.go \
 proto/regatta.pb.go proto/regatta_grpc.pb.go proto/regatta_vtproto.pb.go \
 proto/replication.pb.go proto/replication_grpc.pb.go proto/replication_vtproto.pb.go \
 proto/maintenance.pb.go proto/maintenance_grpc.pb.go proto/maintenance_vtproto.pb.go

proto: $(PROTO_GO_OUTS)

$(PROTO_GO_OUTS): proto/*.proto
	protoc -I proto/ --go_out=. --go-grpc_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool --go-vtproto_opt=pool=./proto.Command --go-vtproto_opt=pool=./proto.SnapshotChunk proto/*.proto --doc_out=./docs --doc_opt=./docs/api.tmpl,api.md

# Build the docker image
.PHONY: docker-build
docker-build: proto
	docker build --build-arg="VERSION=$(VERSION)" . -t $(REPOSITORY):latest -t $(REPOSITORY):$(VERSION)

.PHONY: kind
kind: docker-build kind-cluster
	kubectl --context kind-regatta create namespace regatta || true
	cd helm/regatta && kubecrt charts-kind.yaml | kubectl --context kind-regatta apply -f -
	kubectl -n regatta rollout restart statefulset/regatta

.PHONY: kind-cluster
kind-cluster:
	kind create cluster --config hack/kind-cluster-config.yaml || true
	kind load docker-image --name regatta regatta:latest

.PHONY: clean
clean:
	rm -f regatta

.PHONY: serve-docs
serve-docs:
	BUNDLE_GEMFILE='./docs/Gemfile' bundle exec jekyll serve --source './docs' --config './docs/_config.yml'

