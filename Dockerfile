# syntax = docker/dockerfile:1.2
FROM golang:1.22.3-alpine3.20 as builder

RUN apk add --update --no-cache build-base tzdata \
 && addgroup -g 1000 -S regatta && adduser -u 1000 -S regatta -G regatta

WORKDIR /github.com/jamf/regatta
# Copy the source
COPY . .

# Build
ARG VERSION
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build VERSION=${VERSION} make regatta

# Runtime
FROM alpine:3.20

ARG VERSION
LABEL org.opencontainers.image.authors="Regatta Developers <regatta@jamf.com>"
LABEL org.opencontainers.image.base.name="docker.io/library/alpine:3.19"
LABEL org.opencontainers.image.description="Regatta is a distributed key-value store. It is Kubernetes friendly with emphasis on high read throughput and low operational cost."
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.source="https://github.com/jamf/regatta"
LABEL org.opencontainers.image.version="${VERSION}"

WORKDIR /
COPY --from=builder /etc/passwd /etc/
COPY --from=builder /usr/share/zoneinfo/ /usr/share/zoneinfo/
COPY --from=builder --chown=1000:1000 /github.com/jamf/regatta/regatta /usr/local/bin/

USER regatta

ENTRYPOINT ["regatta"]
