// Copyright JAMF Software, LLC

package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// IsDynamicNode returns if the specified addr uses any kind of SD mechanism (dns, dnssrv, dnssrvnoa).
func IsDynamicNode(addr string) bool {
	qtype, _ := GetQTypeName(addr)
	return qtype != ""
}

// GetQTypeName splits the provided addr into two parts: the QType (if any) and the name.
func GetQTypeName(addr string) (qtype, name string) {
	qtypeAndName := strings.SplitN(addr, "+", 2)
	if len(qtypeAndName) != 2 {
		return "", addr
	}
	return qtypeAndName[0], qtypeAndName[1]
}

type QType string

const (
	// A qtype performs A/AAAA lookup.
	A = QType("dns")
	// SRV qtype performs SRV lookup with A/AAAA lookup for each SRV result.
	SRV = QType("dnssrv")
	// SRVNoA qtype performs SRV lookup without any A/AAAA lookup for each SRV result.
	SRVNoA = QType("dnssrvnoa")
)

type ipLookupResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

// dnsResolver uses Golang default net.Resolver to resolve DNS names.
type dnsResolver struct {
	resolver ipLookupResolver
	logger   *zap.SugaredLogger
}

// Resolve name using net.Resolver.
func (s *dnsResolver) Resolve(ctx context.Context, name string, qtype QType) ([]string, error) {
	var (
		res    []string
		scheme string
	)

	schemeSplit := strings.Split(name, "//")
	if len(schemeSplit) > 1 {
		scheme = schemeSplit[0]
		name = schemeSplit[1]
	}

	// Split the host and port if present.
	host, port, err := net.SplitHostPort(name)
	if err != nil {
		// The host could be missing a port.
		host, port = name, ""
	}

	switch qtype {
	case A:
		if port == "" {
			return nil, fmt.Errorf("missing port in address given for dns lookup: %v", name)
		}
		ips, err := s.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			if !dnsIdNotFound(err) {
				return nil, fmt.Errorf("%w lookup IP addresses %q", err, host)
			}
		}
		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
		}
	case SRV, SRVNoA:
		_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
		if err != nil {
			if !dnsIdNotFound(err) {
				return nil, fmt.Errorf("%w lookup SRV records %q", err, host)
			}
			if len(recs) == 0 {
				s.logger.Errorf("failed to lookup SRV records '%s': %v", host, err)
			}
		}

		for _, rec := range recs {
			// Only use port from SRV record if no explicit port was specified.
			resPort := port
			if resPort == "" {
				resPort = strconv.Itoa(int(rec.Port))
			}

			if qtype == SRVNoA {
				res = append(res, appendScheme(scheme, net.JoinHostPort(rec.Target, resPort)))
				continue
			}
			// Do A lookup for the domain in SRV answer.
			resIPs, err := s.resolver.LookupIPAddr(ctx, rec.Target)
			if err != nil {
				if !dnsIdNotFound(err) {
					return nil, fmt.Errorf("%w lookup IP addresses %q", err, host)
				}
				if len(resIPs) == 0 {
					s.logger.Errorf("failed to lookup IP addresses '%s': %v", host, err)
				}
			}
			for _, resIP := range resIPs {
				res = append(res, appendScheme(scheme, net.JoinHostPort(resIP.String(), resPort)))
			}
		}
	default:
		return nil, fmt.Errorf("invalid lookup scheme %q", qtype)
	}

	if res == nil && err == nil {
		s.logger.Warnf("IP address lookup yielded no results. No host found or no addresses found for '%s'", host)
	}

	return res, nil
}

func dnsIdNotFound(err error) bool {
	if err == nil {
		return false
	}
	err = errors.Unwrap(err)
	dnsErr, ok := err.(*net.DNSError)
	return ok && dnsErr.IsNotFound
}

func appendScheme(scheme, host string) string {
	if scheme == "" {
		return host
	}
	return scheme + "//" + host
}
