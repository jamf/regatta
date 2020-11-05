# Build the regatta binary
FROM golang:1.15.3-alpine3.12 as builder
RUN sed -i -e 's/v3\.3/edge/g' /etc/apk/repositories; \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories && \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/main' >> /etc/apk/repositories && \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories && \
        apk add --update --no-cache rocksdb-dev build-base
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

FROM alpine:3.12
RUN sed -i -e 's/v3\.3/edge/g' /etc/apk/repositories; \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories && \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/main' >> /etc/apk/repositories && \
        echo 'http://dl-4.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories && \
        apk add --update --no-cache bash ca-certificates rocksdb

WORKDIR /
COPY --from=builder /github.com/wandera/regatta/regatta /bin/regatta
ENTRYPOINT ["regatta"]
