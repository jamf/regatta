package cert

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	invalidCertFile = filepath.Join("testdata", "server-invalid.crt")
	invalidKeyFile  = filepath.Join("testdata", "server-invalid.key")
)

func TestWatcher_TLSConfig(t *testing.T) {
	testCertFile := filepath.Join(t.TempDir(), "server.crt")
	testKeyFile := filepath.Join(t.TempDir(), "server.key")

	r := require.New(t)
	w := &Watcher{
		CertFile: testCertFile,
		KeyFile:  testKeyFile,
		Log:      zaptest.NewLogger(t).Sugar(),
	}

	validateCert := func(tlsConf *tls.Config) {
		r.Eventually(func() bool {
			cert := w.GetCertificate()
			return reflect.DeepEqual(tlsConf.Certificates[0], *cert)
		}, 10*time.Second, 250*time.Millisecond, "certificate not loaded")
	}

	t.Log("watch valid cert and key")
	validKeyFile, validCertFile, validTLSConf := createValidTLSPairInDir(t.TempDir(), "valid")
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	r.NoError(w.Watch())
	defer w.Stop()
	validateCert(validTLSConf)

	t.Log("replace with invalid cert and key")
	mustCopyFile(invalidCertFile, testCertFile)
	mustCopyFile(invalidKeyFile, testKeyFile)
	validateCert(validTLSConf)

	t.Log("replace with valid cert and key")
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	validateCert(validTLSConf)

	t.Log("delete files")
	_ = os.Remove(testCertFile)
	_ = os.Remove(testKeyFile)
	validateCert(validTLSConf)

	t.Log("replace with different valid cert and key")
	validKeyFile2, validCertFile2, validTLSConf2 := createValidTLSPairInDir(t.TempDir(), "valid")
	mustCopyFile(validCertFile2, testCertFile)
	mustCopyFile(validKeyFile2, testKeyFile)
	validateCert(validTLSConf2)
}

func TestWatcher_load(t *testing.T) {
	validKeyFile, validCertFile, _ := createValidTLSPairInDir(t.TempDir(), "valid")
	type fields struct {
		CertFile string
		KeyFile  string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "load valid cert and key pair",
			fields: fields{
				CertFile: validCertFile,
				KeyFile:  validKeyFile,
			},
			wantErr: false,
		},
		{
			name: "load invalid cert",
			fields: fields{
				CertFile: invalidCertFile,
				KeyFile:  validKeyFile,
			},
			wantErr: true,
		},
		{
			name: "load invalid key",
			fields: fields{
				CertFile: validCertFile,
				KeyFile:  invalidKeyFile,
			},
			wantErr: true,
		},
		{
			name: "load invalid cert and key pair",
			fields: fields{
				CertFile: invalidCertFile,
				KeyFile:  invalidKeyFile,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			w := &Watcher{
				CertFile: tt.fields.CertFile,
				KeyFile:  tt.fields.KeyFile,
				Log:      zaptest.NewLogger(t).Sugar(),
			}
			if tt.wantErr {
				r.Error(w.load())
			} else {
				r.NoError(w.load())
			}
		})
	}
}

func mustCopyFile(src, dst string) {
	sf, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer sf.Close()
	df, err := os.Create(dst)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(df, sf)
	if err != nil {
		panic(err)
	}
}

func createValidTLSPairInDir(dir string, name string) (string, string, *tls.Config) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	keyFileName := path.Join(dir, fmt.Sprintf("%s.key", name))
	kf, err := os.Create(keyFileName)
	if err != nil {
		panic(err)
	}
	certFileName := path.Join(dir, fmt.Sprintf("%s.crt", name))
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

	return keyFileName, certFileName, tlsConfigFromFile(keyFileName, certFileName)
}

func tlsConfigFromFile(keyFile, certFile string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}
