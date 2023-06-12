// Copyright JAMF Software, LLC

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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	invalidCertFile = filepath.Join("testdata", "server-invalid.crt")
	invalidKeyFile  = filepath.Join("testdata", "server-invalid.key")
)

func TestReloadable_GetCertificate(t *testing.T) {
	testCertFile := filepath.Join(t.TempDir(), "server.crt")
	testKeyFile := filepath.Join(t.TempDir(), "server.key")

	mc := clock.NewMock()
	r := require.New(t)
	w := &Reloadable{
		interval: 1 * time.Second,
		clock:    mc,
		cert:     testCertFile,
		key:      testKeyFile,
	}

	validateCert := func(w *Reloadable, tlsConf *tls.Config) {
		r.EventuallyWithT(func(collect *assert.CollectT) {
			cert, err := w.GetCertificate(nil)
			require.NoError(collect, err)
			require.Equal(collect, tlsConf.Certificates[0], *cert)

			cert, err = w.GetClientCertificate(nil)
			require.NoError(collect, err)
			require.Equal(collect, tlsConf.Certificates[0], *cert)
		}, 10*time.Second, 250*time.Millisecond, "certificate not loaded")
	}

	t.Log("watch valid cert and key")
	validKeyFile, validCertFile, validTLSConf := createValidTLSPairInDir(t.TempDir())
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	validateCert(w, validTLSConf)

	t.Log("replace with invalid cert and key")
	mustCopyFile(invalidCertFile, testCertFile)
	mustCopyFile(invalidKeyFile, testKeyFile)
	validateCert(w, validTLSConf)

	t.Log("replace with valid cert and key")
	mustCopyFile(validCertFile, testCertFile)
	mustCopyFile(validKeyFile, testKeyFile)
	validateCert(w, validTLSConf)

	t.Log("delete files")
	_ = os.Remove(testCertFile)
	_ = os.Remove(testKeyFile)
	validateCert(w, validTLSConf)

	t.Log("replace with different valid cert and key")
	validKeyFile2, validCertFile2, validTLSConf2 := createValidTLSPairInDir(t.TempDir())
	t.Log("advance time")
	mc.Add(w.interval * 2)
	mustCopyFile(validCertFile2, testCertFile)
	mustCopyFile(validKeyFile2, testKeyFile)
	validateCert(w, validTLSConf2)
}

func TestReloadable_reload(t *testing.T) {
	validKeyFile, validCertFile, _ := createValidTLSPairInDir(t.TempDir())
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
			w := &Reloadable{
				cert:  tt.fields.CertFile,
				key:   tt.fields.KeyFile,
				clock: clock.NewMock(),
			}
			if tt.wantErr {
				r.Error(w.reload())
			} else {
				r.NoError(w.reload())
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

func createValidTLSPairInDir(dir string) (string, string, *tls.Config) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
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

	return keyFileName, certFileName, tlsConfigFromFile(keyFileName, certFileName)
}

func tlsConfigFromFile(keyFile, certFile string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func TestNew(t *testing.T) {
	validKeyFile, validCertFile, _ := createValidTLSPairInDir(t.TempDir())
	type args struct {
		cerFile string
		keyFile string
	}
	tests := []struct {
		name    string
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "invalid certs",
			args:    args{cerFile: invalidCertFile, keyFile: invalidKeyFile},
			wantErr: require.Error,
		},
		{
			name:    "valid certs",
			args:    args{cerFile: validCertFile, keyFile: validKeyFile},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.args.cerFile, tt.args.keyFile)
			tt.wantErr(t, err, fmt.Sprintf("New(%v, %v)", tt.args.cerFile, tt.args.keyFile))
		})
	}
}
