# Image URL to use all building/pushing image targets
IMG ?= regatta:latest

all: clean check test build

check:
	go vet ./...
	go fmt ./...

test:
	go test ./...

build:
	CGO_ENABLED=0 go build -a -o regatta
# Build the docker image
docker-build:
	docker build . -t ${IMG}

clean:
	rm -f regatta
