// Copyright JAMF Software, LLC

package kv

import (
	"errors"
	"path"
	"strings"
)

var (
	// ErrNotExist key does not exist in the store.
	ErrNotExist = errors.New("key does not exist")
	// ErrVersionMismatch version provided does not match the version stored.
	ErrVersionMismatch = errors.New("version mismatch")
)

// Pair represents a single versioned KV pair.
type Pair struct {
	// Key a full path in KV store.
	Key string
	// Value a value assigned to the Key.
	Value string
	// Ver a key version, version must be supplied for consecutive updates.
	Ver uint64
}

func stripKey(key, prefix string) string {
	return strings.TrimPrefix(strings.TrimPrefix(key, prefix), "/")
}

func pathToTerms(filePath string) []string {
	return strings.Split(path.Clean(filePath), "/")
}

func samePrefixTerms(prefix, test []string) bool {
	if len(test) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if prefix[i] != test[i] {
			return false
		}
	}
	return true
}
