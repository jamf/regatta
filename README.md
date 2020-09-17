# Regatta
Regatta is a distributed key-value store that is deployed in the the core and each EDGE location.

## Development environment prerequisites
* [Go](https://golang.org/) >= 1.15 -- `brew install go`
* Protocol Buffer compiler >= 3 -- `brew install protobuf`
* Go plugin for the protocol compiler -- `GO111MODULE=on go get github.com/golang/protobuf/protoc-gen-go`
* Docker (optional)

## Run server
`make run`

# Run client
`make run-client`
