// Copyright JAMF Software, LLC

package cert

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// Reloadable represents a certificate able to reflect certificate and key pairs changes.
type Reloadable struct {
	mu sync.Mutex

	cert string
	key  string

	interval   time.Duration
	lastReload time.Time

	keyPair *tls.Certificate
	clock   clock.Clock
}

func New(certFile, keyFile string) (*Reloadable, error) {
	r := &Reloadable{
		cert:     certFile,
		key:      keyFile,
		interval: 1 * time.Minute,
		clock:    clock.New(),
	}
	return r, r.reload()
}

// GetCertificate returns a TLS keypair built from the watched files. In case of a reload failure it returns the last correctly loaded value.
func (w *Reloadable) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	return w.getCertificate(), nil
}

// GetClientCertificate returns a TLS keypair built from the watched files. In case of a reload failure it returns the last correctly loaded value.
func (w *Reloadable) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return w.getCertificate(), nil
}

func (w *Reloadable) getCertificate() *tls.Certificate {
	w.mu.Lock()
	defer w.mu.Unlock()
	// If interval passed since the last check, attempt certificate reload.
	if w.lastReload.Add(w.interval).Before(w.clock.Now()) {
		// After the initial load we are not interested in errors.
		_ = w.reload()
	}
	return w.keyPair
}

func (w *Reloadable) reload() error {
	keyPair, err := tls.LoadX509KeyPair(w.cert, w.key)
	if err != nil {
		return err
	}
	w.keyPair = &keyPair
	w.lastReload = w.clock.Now()
	return nil
}
