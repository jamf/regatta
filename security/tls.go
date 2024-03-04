// Copyright JAMF Software, LLC

package security

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"go.uber.org/zap"
)

type TLSInfo struct {
	// CertFile is the _server_ cert, it will also be used as a _client_ certificate if ClientCertFile is empty
	CertFile string
	// KeyFile is the key for the CertFile
	KeyFile string
	// ClientCertFile is a _client_ cert for initiating connections when ClientCertAuth is defined. If ClientCertAuth
	// is true but this value is empty, the CertFile will be used instead.
	ClientCertFile string
	// ClientKeyFile is the key for the ClientCertFile.
	ClientKeyFile string

	TrustedCAFile      string
	ClientCertAuth     bool
	InsecureSkipVerify bool

	// ServerName ensures the cert matches the given host in case of discovery / virtual hosting.
	ServerName string

	// HandshakeFailure is optionally called when a connection fails to handshake. The
	// connection will be closed immediately afterwards.
	HandshakeFailure func(*tls.Conn, error)

	// CipherSuites is a list of supported cipher suites.
	// If empty, Go auto-populates it by default.
	// Note that cipher suites are prioritized in the given order.
	CipherSuites []uint16

	// MaxVersion is the maximum TLS version that is acceptable.
	// If not set, the default used by Go is selected (see tls.Config.MaxVersion).
	MaxVersion uint16

	// parseFunc exists to simplify testing. Typically, parseFunc
	// should be left nil. In that case, tls.X509KeyPair will be used.
	parseFunc func([]byte, []byte) (tls.Certificate, error)

	// AllowedCN is a CN which must be provided by a client.
	AllowedCN string

	// AllowedHostname is an IP address or hostname that must match the TLS
	// certificate provided by a client.
	AllowedHostname string

	// Logger logs TLS errors.
	// If nil, all logs are discarded.
	Logger *zap.SugaredLogger

	// EmptyCN indicates that the cert must have empty CN.
	// If true, ClientConfig() will return an error for a cert with non empty CN.
	EmptyCN bool
}

func (t TLSInfo) String() string {
	return fmt.Sprintf("cert=%s, key=%s, client-cert=%s, client-key=%s, trusted-ca=%s, client-cert-auth=%v", t.CertFile, t.KeyFile, t.ClientCertFile, t.ClientKeyFile, t.TrustedCAFile, t.ClientCertAuth)
}

func (t TLSInfo) Empty() bool {
	return t.CertFile == "" && t.KeyFile == ""
}

func (t TLSInfo) baseConfig() (*tls.Config, error) {
	if t.KeyFile == "" || t.CertFile == "" {
		return nil, fmt.Errorf("KeyFile and CertFile must both be present[key: %v, cert: %v]", t.KeyFile, t.CertFile)
	}
	if t.Logger == nil {
		t.Logger = zap.NewNop().Sugar()
	}

	_, err := NewCert(t.CertFile, t.KeyFile, t.parseFunc)
	if err != nil {
		return nil, err
	}

	if (t.ClientKeyFile == "") != (t.ClientCertFile == "") {
		return nil, fmt.Errorf("ClientKeyFile and ClientCertFile must both be present or both absent: key: %v, cert: %v]", t.ClientKeyFile, t.ClientCertFile)
	}
	if t.ClientCertFile != "" {
		_, err := NewCert(t.ClientCertFile, t.ClientKeyFile, t.parseFunc)
		if err != nil {
			return nil, err
		}
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: t.MaxVersion,
		ServerName: t.ServerName,
	}

	if len(t.CipherSuites) > 0 {
		cfg.CipherSuites = t.CipherSuites
	}

	// Client certificates may be verified by either an exact match on the CN,
	// or a more general check of the CN and SANs.
	var verifyCertificate func(*x509.Certificate) bool
	if t.AllowedCN != "" {
		if t.AllowedHostname != "" {
			return nil, fmt.Errorf("AllowedCN and AllowedHostname are mutually exclusive (cn=%q, hostname=%q)", t.AllowedCN, t.AllowedHostname)
		}
		verifyCertificate = func(cert *x509.Certificate) bool {
			return t.AllowedCN == cert.Subject.CommonName
		}
	}
	if t.AllowedHostname != "" {
		verifyCertificate = func(cert *x509.Certificate) bool {
			return cert.VerifyHostname(t.AllowedHostname) == nil
		}
	}
	if verifyCertificate != nil {
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			for _, chains := range verifiedChains {
				if len(chains) != 0 {
					if verifyCertificate(chains[0]) {
						return nil
					}
				}
			}
			return errors.New("client certificate authentication failed")
		}
	}

	// this only reloads certs when there's a client request
	cfg.GetCertificate = func(clientHello *tls.ClientHelloInfo) (cert *tls.Certificate, err error) {
		cert, err = NewCert(t.CertFile, t.KeyFile, t.parseFunc)
		if err != nil {
			if os.IsNotExist(err) {
				t.Logger.Warnf(
					"failed to find peer cert files: cert-file=%s, key-file=%s, err=%v",
					t.CertFile, t.KeyFile, err,
				)
			} else {
				t.Logger.Warnf(
					"failed to create peer certificate: cert-file=%s, key-file=%s, err=%v",
					t.CertFile, t.KeyFile, err,
				)
			}
		}
		return cert, err
	}
	cfg.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (cert *tls.Certificate, err error) {
		certfile, keyfile := t.CertFile, t.KeyFile
		if t.ClientCertFile != "" {
			certfile, keyfile = t.ClientCertFile, t.ClientKeyFile
		}
		cert, err = NewCert(certfile, keyfile, t.parseFunc)
		if err != nil {
			if os.IsNotExist(err) {
				t.Logger.Warnf(
					"failed to find client cert files: cert-file=%s, key-file=%s, err=%v",
					t.CertFile, t.KeyFile, err,
				)
			} else {
				t.Logger.Warnf(
					"failed to create client certificate: cert-file=%s, key-file=%s, err=%v",
					t.CertFile, t.KeyFile, err,
				)
			}
		}
		return cert, err
	}
	return cfg, nil
}

// cafiles returns a list of CA file paths.
func (t TLSInfo) cafiles() []string {
	cs := make([]string, 0)
	if t.TrustedCAFile != "" {
		cs = append(cs, t.TrustedCAFile)
	}
	return cs
}

// ServerConfig generates a tls.Config object for use by an HTTP server.
func (t TLSInfo) ServerConfig() (*tls.Config, error) {
	cfg, err := t.baseConfig()
	if err != nil {
		return nil, err
	}

	if t.Logger == nil {
		t.Logger = zap.NewNop().Sugar()
	}

	cfg.ClientAuth = tls.NoClientCert
	if t.TrustedCAFile != "" || t.ClientCertAuth {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	cs := t.cafiles()
	if len(cs) > 0 {
		cp, err := NewCertPool(cs)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = cp
	}

	// "h2" NextProtos is necessary for enabling HTTP2 for go's HTTP server
	cfg.NextProtos = []string{"h2"}

	t.Logger.Infof("server config: %s", t)

	return cfg, nil
}

// ClientConfig generates a tls.Config object for use by an HTTP client.
func (t TLSInfo) ClientConfig() (*tls.Config, error) {
	var cfg *tls.Config
	var err error

	if !t.Empty() {
		cfg, err = t.baseConfig()
		if err != nil {
			return nil, err
		}
	} else {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	}
	cfg.InsecureSkipVerify = t.InsecureSkipVerify

	cs := t.cafiles()
	if len(cs) > 0 {
		cfg.RootCAs, err = NewCertPool(cs)
		if err != nil {
			return nil, err
		}
	}

	if t.EmptyCN {
		hasNonEmptyCN := false
		cn := ""
		_, err := NewCert(t.CertFile, t.KeyFile, func(certPEMBlock []byte, keyPEMBlock []byte) (tls.Certificate, error) {
			var block *pem.Block
			block, _ = pem.Decode(certPEMBlock)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return tls.Certificate{}, err
			}
			if len(cert.Subject.CommonName) != 0 {
				hasNonEmptyCN = true
				cn = cert.Subject.CommonName
			}
			return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		})
		if err != nil {
			return nil, err
		}
		if hasNonEmptyCN {
			return nil, fmt.Errorf("cert has non empty Common Name (%s): %s", cn, t.CertFile)
		}
	}

	t.Logger.Infof("client config: %s", t)

	return cfg, nil
}

// NewCertPool creates x509 certPool with provided CA files.
func NewCertPool(CAFiles []string) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()

	for _, CAFile := range CAFiles {
		pemByte, err := os.ReadFile(CAFile)
		if err != nil {
			return nil, err
		}

		for {
			var block *pem.Block
			block, pemByte = pem.Decode(pemByte)
			if block == nil {
				break
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}

			certPool.AddCert(cert)
		}
	}

	return certPool, nil
}

// NewCert generates TLS cert by using the given cert,key and parse function.
func NewCert(certfile, keyfile string, parseFunc func([]byte, []byte) (tls.Certificate, error)) (*tls.Certificate, error) {
	cert, err := os.ReadFile(certfile)
	if err != nil {
		return nil, err
	}

	key, err := os.ReadFile(keyfile)
	if err != nil {
		return nil, err
	}

	if parseFunc == nil {
		parseFunc = tls.X509KeyPair
	}

	tlsCert, err := parseFunc(cert, key)
	if err != nil {
		return nil, err
	}
	return &tlsCert, nil
}
