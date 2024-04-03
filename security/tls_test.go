// Copyright JAMF Software, LLC

package security

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTLSInfo_NotExist(t *testing.T) {
	tlsInfo := TLSInfo{CertFile: "@badname", KeyFile: "@badname"}
	_, err := tlsInfo.ServerConfig()
	werr := &os.PathError{
		Op:   "open",
		Path: "@badname",
		Err:  errors.New("no such file or directory"),
	}
	require.EqualError(t, err, werr.Error())
}

func TestTLSInfo_Empty(t *testing.T) {
	tests := []struct {
		info TLSInfo
		want bool
	}{
		{TLSInfo{}, true},
		{TLSInfo{TrustedCAFile: "baz"}, true},
		{TLSInfo{CertFile: "foo"}, false},
		{TLSInfo{KeyFile: "bar"}, false},
		{TLSInfo{CertFile: "foo", KeyFile: "bar"}, false},
		{TLSInfo{CertFile: "foo", TrustedCAFile: "baz"}, false},
		{TLSInfo{KeyFile: "bar", TrustedCAFile: "baz"}, false},
		{TLSInfo{CertFile: "foo", KeyFile: "bar", TrustedCAFile: "baz"}, false},
	}
	for _, tt := range tests {
		got := tt.info.Empty()
		require.Equal(t, tt.want, got)
	}
}

func TestTLSInfo_ServerConfig(t *testing.T) {
	tests := []struct {
		name    string
		creator func() TLSInfo
		assert  func(t *testing.T, cfg TLSInfo)
	}{
		{
			name: "basic server config",
			creator: func() TLSInfo {
				cert, key, _ := createValidTLSPairInDir(t.TempDir())
				return TLSInfo{
					CertFile:           cert,
					KeyFile:            key,
					InsecureSkipVerify: true,
					Logger:             zap.NewNop().Sugar(),
				}
			},
			assert: func(t *testing.T, cfg TLSInfo) {
				sc, err := cfg.ServerConfig()
				require.NoError(t, err)
				cc, err := cfg.ClientConfig()
				require.NoError(t, err)
				testConnection(t, sc, cc)
			},
		},
		{
			name: "empty CN server config",
			creator: func() TLSInfo {
				cert, key, _ := createValidTLSPairInDir(t.TempDir())
				return TLSInfo{
					CertFile:           cert,
					KeyFile:            key,
					InsecureSkipVerify: true,
					Logger:             zap.NewNop().Sugar(),
					EmptyCN:            true,
				}
			},
			assert: func(t *testing.T, cfg TLSInfo) {
				sc, err := cfg.ServerConfig()
				require.NoError(t, err)
				cc, err := cfg.ClientConfig()
				require.NoError(t, err)
				testConnection(t, sc, cc)
			},
		},
		{
			name: "mTLS config",
			creator: func() TLSInfo {
				cert, key, ca := createValidTLSPairInDir(t.TempDir())
				return TLSInfo{
					CertFile:           cert,
					KeyFile:            key,
					TrustedCAFile:      ca,
					InsecureSkipVerify: true,
					Logger:             zap.NewNop().Sugar(),
					ClientCertAuth:     true,
					ServerName:         "localhost",
				}
			},
			assert: func(t *testing.T, cfg TLSInfo) {
				sc, err := cfg.ServerConfig()
				require.NoError(t, err)
				cc, err := cfg.ClientConfig()
				require.NoError(t, err)
				testConnection(t, sc, cc)
			},
		},
		{
			name: "invalid server config",
			creator: func() TLSInfo {
				return TLSInfo{
					CertFile:           "foo",
					KeyFile:            "bar",
					InsecureSkipVerify: true,
					Logger:             zap.NewNop().Sugar(),
				}
			},
			assert: func(t *testing.T, cfg TLSInfo) {
				_, err := cfg.ServerConfig()
				require.Error(t, err)
			},
		},
		{
			name: "empty server config",
			creator: func() TLSInfo {
				return TLSInfo{}
			},
			assert: func(t *testing.T, cfg TLSInfo) {
				_, err := cfg.ServerConfig()
				require.Error(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.creator()
			if tt.assert != nil {
				tt.assert(t, cfg)
			}
		})
	}
}

func testConnection(t *testing.T, sc, cc *tls.Config) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l = tls.NewListener(l, sc)
	hs := &http.Server{
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(200)
		}),
		TLSConfig: sc,
	}
	defer hs.Close()
	go hs.Serve(l)

	d, err := net.Dial(l.Addr().Network(), l.Addr().String())
	conn := tls.Client(d, cc)
	hc := &http.Client{Transport: &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
		},
		TLSClientConfig: cc,
	}}
	r, err := hc.Get("https://localhost/")
	require.NoError(t, err)
	require.Equal(t, 200, r.StatusCode)
}

func createValidTLSPairInDir(dir string) (string, string, string) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &caKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}
	caFileName := path.Join(dir, "ca.crt")
	caf, err := os.Create(caFileName)
	if err != nil {
		panic(err)
	}
	if err := pem.Encode(caf, &pem.Block{Type: "CERTIFICATE", Bytes: caDER}); err != nil {
		panic(err)
	}

	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		panic(err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	template = x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, ca, &key.PublicKey, caKey)
	if err != nil {
		panic(err)
	}

	keyFileName := path.Join(dir, "valid.key")
	kf, err := os.Create(keyFileName)
	if err != nil {
		panic(err)
	}
	certFileName := path.Join(dir, "valid.crt")
	cf, err := os.Create(certFileName)
	if err != nil {
		panic(err)
	}
	if err := pem.Encode(kf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		panic(err)
	}
	if err := pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		panic(err)
	}

	return certFileName, keyFileName, caFileName
}
