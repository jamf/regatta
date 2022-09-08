# syntax = docker/dockerfile:1.2
# Build the regatta binary
FROM golang:1.19-alpine3.15 as builder
RUN apk add --update --no-cache build-base
WORKDIR /github.com/wandera/regatta
# Copy the source
COPY . ./
# Build
RUN --mount=type=cache,target=/go/pkg/mod \
--mount=type=cache,target=/root/.cache/go-build \
GOMODCACHE=/go/pkg/mod \
GOCACHE=/root/.cache/go-build \
CGO_ENABLED=1 go build -o regatta

# Runtime
FROM alpine:3.15 as runtime
RUN apk add --update --no-cache bash ca-certificates
WORKDIR /
COPY --from=builder /github.com/wandera/regatta/regatta /bin/regatta
ENTRYPOINT ["regatta"]
