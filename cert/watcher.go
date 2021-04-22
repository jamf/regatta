package cert

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// A Watcher represents a certificate manager able to watch certificate and key pairs for changes.
type Watcher struct {
	mu       sync.RWMutex
	CertFile string
	KeyFile  string
	keyPair  *tls.Certificate
	watcher  *fsnotify.Watcher
	watching chan bool
	Log      Logger
}

// Logger is an interface that wraps the basic logger methods.
type Logger interface {
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Errorf(string, ...interface{})
}

// Watch starts watching for changes to the certificate and key files. On any change the certificate and key
// are reloaded. If there is an issue the load will fail and the old certificate and key will continue to be served.
func (w *Watcher) Watch() error {
	var err error
	if w.watcher, err = fsnotify.NewWatcher(); err != nil {
		return fmt.Errorf("can't create watcher: %w", err)
	}
	if err = w.watcher.Add(w.CertFile); err != nil {
		return fmt.Errorf("can't watch cert file: %w", err)
	}
	if err = w.watcher.Add(w.KeyFile); err != nil {
		return fmt.Errorf("can't watch key file: %w", err)
	}
	if err := w.load(); err != nil {
		return fmt.Errorf("can't load cert or key file: %w", err)
	}
	w.watching = make(chan bool)
	go w.run()
	return nil
}

func (w *Watcher) load() error {
	keyPair, err := tls.LoadX509KeyPair(w.CertFile, w.KeyFile)
	if err == nil {
		w.mu.Lock()
		w.keyPair = &keyPair
		w.mu.Unlock()
		w.Log.Infof("certificate and key loaded")
	}
	return err
}

func (w *Watcher) run() {
loop:
	for {
		select {
		case <-w.watching:
			break loop
		case event := <-w.watcher.Events:
			w.Log.Debugf("watch event: %v", event)
			if err := w.load(); err != nil {
				w.Log.Errorf("can't load cert or key file: %v", err)
			}
		case err := <-w.watcher.Errors:
			w.Log.Debugf("error watching files: %v", err)
		}
	}
	w.Log.Infof("stopped watching")
	_ = w.watcher.Close()
}

// Stop tells Watcher to stop watching for changes to the certificate and key files.
func (w *Watcher) Stop() {
	w.watching <- false
}

// TLSConfig creates a new dynamically loaded tls.Config, in which changes to the certificate are reflected in.
func (w *Watcher) TLSConfig() *tls.Config {
	return &tls.Config{GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
		w.mu.RLock()
		defer w.mu.RUnlock()
		return w.keyPair, nil
	}}
}