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

// Pairs slice of Pair that could be sorted.
type Pairs []Pair

// Len is the number of elements in the collection.
func (p Pairs) Len() int {
	return len(p)
}

// Less reports whether the element with index i must sort before the element with index j.
func (p Pairs) Less(i, j int) bool {
	return p[i].Key < p[j].Key
}

// Swap swaps the elements with indexes i and j.
func (p Pairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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
