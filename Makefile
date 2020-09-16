# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: clean check test build

# Run golangci-lint on the code
check:
	@echo "Running check"
ifeq (, $(shell which golangci-lint))
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.30.0
endif
	golangci-lint run

test:
	go test ./...

build:
	CGO_ENABLED=0 go build -a -o regatta
# Build the docker image
docker-build:
	docker build . -t ${IMG}

clean:
	rm -f regatta
