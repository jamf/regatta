// Copyright JAMF Software, LLC

package gzip

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/test/bufconn"
)

const (
	bufSize = 1024
	message = "This is the gzip response"
)

type testServer struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (t *testServer) UnaryCall(_ context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{Payload: &grpc_testing.Payload{
		Body: req.Payload.Body,
	}}, nil
}

func TestRegisteredCompression(t *testing.T) {
	comp := encoding.GetCompressor(Name)
	require.NotNil(t, comp)
	require.Equal(t, Name, comp.Name())

	buf := bytes.NewBuffer(make([]byte, 0, bufSize))
	wc, err := comp.Compress(buf)
	require.NoError(t, err)

	_, err = wc.Write([]byte(message))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	r, err := comp.Decompress(buf)
	require.NoError(t, err)
	expected, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, message, string(expected))
}

func TestRoundTrip(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		require.NoError(t, lis.Close())
	})

	done := make(chan struct{}, 1)
	s := grpc.NewServer()
	defer func() {
		s.GracefulStop()
		<-done
	}()

	go func() {
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("server exited with error: %v", err)
		}
		done <- struct{}{}
	}()

	grpc_testing.RegisterTestServiceServer(s, &testServer{})

	conn, err := grpc.NewClient(":0",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(Name)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	client := grpc_testing.NewTestServiceClient(conn)
	resp, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{Payload: &grpc_testing.Payload{Body: []byte(message)}})
	require.NoError(t, err)
	require.Equal(t, message, string(resp.Payload.Body))
}
