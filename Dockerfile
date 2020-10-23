# Build the regatta binary
FROM golang:1.15.2 as builder

WORKDIR /workspace

# This will cache the go mod download step, unless go.mod or go.sum changes
# Copy the Go Modules manifests
COPY go.mod go.sum ./

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY . ./

# Build
RUN CGO_ENABLED=0 go build -a -o regatta

FROM alpine:3.12
RUN apk add --no-cache bash ca-certificates

WORKDIR /
COPY --from=builder /workspace/regatta /bin/regatta
ENTRYPOINT ["regatta"]
