# syntax = docker/dockerfile:1.2
# Build the regatta binary
FROM golang:1.18-alpine3.15 as builder
RUN apk add --update --no-cache build-base
WORKDIR /github.com/wandera/regatta
# This will cache the go mod download step, unless go.mod or go.sum changes
# Copy the Go Modules manifests
COPY go.mod go.sum ./
# This will cache the go mod download step, unless go.mod or go.sum changes
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
# Copy the source
COPY . ./
# Build
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=1 go build -o regatta

# Runtime
FROM alpine:3.15 as runtime
RUN apk add --update --no-cache bash ca-certificates
WORKDIR /
COPY --from=builder /github.com/wandera/regatta/regatta /bin/regatta
ENTRYPOINT ["regatta"]
