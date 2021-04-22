# Build the regatta binary
FROM golang:1.16 as builder
RUN apt-get update -y --fix-missing && apt-get install -y librocksdb-dev

WORKDIR /github.com/wandera/regatta

# This will cache the go mod download step, unless go.mod or go.sum changes
# Copy the Go Modules manifests
COPY go.mod go.sum ./

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY . ./

# Build
RUN CGO_ENABLED=1 go build -a -o regatta

# Runtime
FROM debian:10-slim as runtime
RUN apt-get update -y --fix-missing && apt-get install -y bash librocksdb5.17 && rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=builder /github.com/wandera/regatta/regatta /bin/regatta
ENTRYPOINT ["regatta"]
