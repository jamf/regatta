# syntax = docker/dockerfile:1.2
# Build the regatta binary
FROM golang:1.21.4-alpine3.18 as builder

ARG VERSION

RUN apk add --update --no-cache build-base
WORKDIR /github.com/jamf/regatta
# Copy the source
COPY . ./
# Build
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build VERSION=${VERSION} make regatta

# Runtime
FROM alpine:3.18 as runtime

LABEL maintainer="Regatta Developers regatta@jamf.com"
LABEL desc="Regatta is a distributed key-value store"

RUN apk add --update --no-cache bash ca-certificates
WORKDIR /
COPY --from=builder /github.com/jamf/regatta/regatta /bin/regatta
ENTRYPOINT ["regatta"]
