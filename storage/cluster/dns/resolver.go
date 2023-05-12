// Copyright JAMF Software, LLC

package dns

import (
	"context"
	"errors"
	"net"
	"sync"

	"go.uber.org/zap"
)

type resolver interface {
	Resolve(ctx context.Context, name string, qtype QType) ([]string, error)
}

// Resolver is a stateful cache for asynchronous DNS resolutions. It provides a way to resolve addresses and obtain them.
type Resolver struct {
	resolver resolver
	// A map from domain name to a slice of resolved targets.
	cache struct {
		sync.RWMutex
		data map[string][]string
	}
}

// NewResolver returns a new dns resolver backed by stdlib net.Resolver.
func NewResolver(logger *zap.SugaredLogger) *Resolver {
	p := &Resolver{
		resolver: &dnsResolver{resolver: &net.Resolver{}, logger: logger},
		cache: struct {
			sync.RWMutex
			data map[string][]string
		}{data: make(map[string][]string)},
	}
	return p
}

// Resolve stores a list of provided addresses or their DNS records if requested.
// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
// For non-SRV records, it will return an error if a port is not supplied.
func (p *Resolver) Resolve(ctx context.Context, addrs []string) error {
	resolvedAddrs := map[string][]string{}
	var lookupErrs []error

	for _, addr := range addrs {
		var resolved []string
		qtype, name := GetQTypeName(addr)
		if qtype == "" {
			resolvedAddrs[name] = []string{name}
			continue
		}

		resolved, err := p.resolver.Resolve(ctx, name, QType(qtype))
		if err != nil {
			// Append all the failed dns resolution in the error list.
			lookupErrs = append(lookupErrs, err)
			// Use cached values.
			p.cache.RLock()
			resolved = p.cache.data[addr]
			p.cache.RUnlock()
		}
		resolvedAddrs[addr] = resolved
	}

	// All addresses have been resolved. We can now take an exclusive lock to
	// update the local state.
	p.cache.Lock()
	defer p.cache.Unlock()
	p.cache.data = resolvedAddrs

	return errors.Join(lookupErrs...)
}

// Addresses returns the latest addresses present in the Resolver.
func (p *Resolver) Addresses() []string {
	p.cache.RLock()
	defer p.cache.RUnlock()

	var result []string
	for _, addrs := range p.cache.data {
		result = append(result, addrs...)
	}
	return result
}
