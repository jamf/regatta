package cert

import (
	"crypto/tls"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	validCertFile   = filepath.Join("testdata", "server-valid.crt")
	validKeyFile    = filepath.Join("testdata", "server-valid.key")
	invalidCertFile = filepath.Join("testdata", "server-invalid.crt")
	invalidKeyFile  = filepath.Join("testdata", "server-invalid.key")
)

func TestWatcher_TLSConfig(t *testing.T) {
	validTLSConf := tlsConfigFromFile(filepath.Join("testdata", "server-valid.crt"), filepath.Join("testdata", "server-valid.key"))

	testCertFile := filepath.Join(t.TempDir(), "server.crt")
	testKeyFile := filepath.Join(t.TempDir(), "server.key")

	r := require.New(t)
	w := &Watcher{
		CertFile: testCertFile,
		KeyFile:  testKeyFile,
		Log:      zaptest.NewLogger(t).Sugar(),
	}

	validateCert := func() {
		time.Sleep(250 * time.Millisecond)
		cert, err := w.TLSConfig().GetCertificate(nil)
		r.NoError(err)
		r.Equal(validTLSConf.Certificates[0], *cert)
	}

	t.Log("watch empty cert and key fail should fail")
	r.Error(w.Watch())

	t.Log("watch valid cert and key")
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	r.NoError(w.Watch())
	defer w.Stop()
	validateCert()

	t.Log("replace with invalid cert and key")
	mustCopyFile(invalidCertFile, testCertFile)
	mustCopyFile(invalidKeyFile, testKeyFile)
	validateCert()

	t.Log("replace with valid cert and key")
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	validateCert()
}

func mustCopyFile(src, dst string) {
	sf, _ := os.Open(src)
	defer sf.Close()
	df, _ := os.Create(dst)
	_, _ = io.Copy(df, sf)
}

func tlsConfigFromFile(certFile, keyFile string) *tls.Config {
	cert, _ := tls.LoadX509KeyPair(certFile, keyFile)
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func TestWatcher_load(t *testing.T) {
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
