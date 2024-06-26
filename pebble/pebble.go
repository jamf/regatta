// Copyright JAMF Software, LLC

package pebble

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	// levels is number of Pebble levels.
	levels = 7
	// targetFileSizeBase base file size (in L0).
	targetFileSizeBase = 16 * 1024 * 1024
	// blockSize FS block size.
	blockSize = 32 * 1024
	// indexBlockSize is a size of index block within each sstable.
	indexBlockSize = 256 * 1024
	// targetFileSizeGrowFactor the factor of growth of targetFileSizeBase between levels.
	targetFileSizeGrowFactor = 2
	// writeBufferSize inmemory write buffer size.
	writeBufferSize = 16 * 1024 * 1024
	// maxWriteBufferNumber number of write buffers.
	maxWriteBufferNumber = 4
	// maxOpenFiles number of max open files per pebble instance.
	maxOpenFiles = 1000
	// l0FileNumCompactionTrigger number of files in L0 to trigger automatic compaction.
	l0FileNumCompactionTrigger = 8
	// l0StopWritesTrigger number of files in L0 to stop accepting more writes.
	l0StopWritesTrigger = 256
	// maxBytesForLevelBase base for amount of data stored in a single level.
	maxBytesForLevelBase = 64 * 1024 * 1024
)

func split(b []byte) int {
	return len(b)
}

func DefaultOptions() *pebble.Options {
	lvlOpts := make([]pebble.LevelOptions, levels)
	sz := targetFileSizeBase
	for l := int64(0); l < levels; l++ {
		opt := pebble.LevelOptions{
			BlockSize:      blockSize,
			Compression:    pebble.SnappyCompression,
			FilterPolicy:   bloom.FilterPolicy(10),
			FilterType:     pebble.TableFilter,
			IndexBlockSize: indexBlockSize,
			TargetFileSize: int64(sz),
		}
		sz *= targetFileSizeGrowFactor
		opt.EnsureDefaults()
		lvlOpts[l] = opt
	}
	// Do not create bloom filters for the last level (i.e. the largest level
	// which contains data in the LSM store). This configuration reduces the size
	// of the bloom filters by 10x. This is significant given that bloom filters
	// require 1.25 bytes (10 bits) per key which can translate into 100s of megabytes of
	// memory given typical key and value sizes. The downside is that bloom
	// filters will only be usable on the higher levels, but that seems
	// acceptable. We'll achieve 80-90% of the benefit of having bloom filters on every level for only 10% of the
	// memory cost.
	lvlOpts[len(lvlOpts)-1].FilterPolicy = nil
	opts := &pebble.Options{
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		L0CompactionFileThreshold:   l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		LBaseMaxBytes:               maxBytesForLevelBase,
		Levels:                      lvlOpts,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		DisableWAL:                  true,
		MaxOpenFiles:                maxOpenFiles,
		Comparer: &pebble.Comparer{
			Compare:            pebble.DefaultComparer.Compare,
			Equal:              pebble.DefaultComparer.Equal,
			AbbreviatedKey:     pebble.DefaultComparer.AbbreviatedKey,
			FormatKey:          pebble.DefaultComparer.FormatKey,
			FormatValue:        pebble.DefaultComparer.FormatValue,
			Separator:          pebble.DefaultComparer.Separator,
			Split:              split,
			Successor:          pebble.DefaultComparer.Successor,
			ImmediateSuccessor: pebble.DefaultComparer.ImmediateSuccessor,
			Name:               pebble.DefaultComparer.Name,
		},
	}
	opts.EnsureDefaults()
	opts.Experimental.EnableValueBlocks = func() bool { return true }
	opts.Experimental.IngestSplit = func() bool { return true }
	return opts
}

func WriterOptions(level int) sstable.WriterOptions {
	return DefaultOptions().MakeWriterOptions(level, sstable.TableFormatPebblev4)
}

type Option interface {
	apply(options *pebble.Options)
}

type funcOption struct {
	f func(options *pebble.Options)
}

func (fdo *funcOption) apply(do *pebble.Options) {
	fdo.f(do)
}

func WithFS(fs vfs.FS) Option {
	return &funcOption{func(options *pebble.Options) {
		options.FS, _ = vfs.WithDiskHealthChecks(fs, 5*time.Second, func(info pebble.DiskSlowInfo) {
			if options.EventListener != nil {
				options.EventListener.DiskSlow(info)
			}
		})
	}}
}

func WithCache(cache *pebble.Cache) Option {
	return &funcOption{func(options *pebble.Options) {
		options.Cache = cache
	}}
}

func WithTableCache(cache *pebble.TableCache) Option {
	return &funcOption{func(options *pebble.Options) {
		options.TableCache = cache
	}}
}

func WithLogger(logger pebble.Logger) Option {
	return &funcOption{func(options *pebble.Options) {
		options.Logger = logger
	}}
}

func WithEventListener(listener pebble.EventListener) Option {
	return &funcOption{func(options *pebble.Options) {
		options.AddEventListener(listener)
	}}
}

// OpenDB opens DB on paths given (using sane defaults).
func OpenDB(dbdir string, options ...Option) (*pebble.DB, error) {
	opts := DefaultOptions()
	for _, option := range options {
		option.apply(opts)
	}
	db, err := pebble.Open(dbdir, opts)
	if err != nil {
		return nil, fmt.Errorf("error opening DB: %w", err)
	}
	err = db.RatchetFormatMajorVersion(pebble.FormatVirtualSSTables)
	if err != nil {
		return nil, fmt.Errorf("error ratcheting DB: %w", err)
	}
	return db, nil
}
