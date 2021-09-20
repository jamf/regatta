package pebble

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
)

const (
	// currentDBFilename static filename with current Pebble DB directory name.
	currentDBFilename string = "current"
	// updatingDBFilename static filename with recovering Pebble DB directory name (before the switch).
	updatingDBFilename string = "current.updating"
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
	// l0FileNumCompactionTrigger number of files in L0 to trigger automatic compaction.
	l0FileNumCompactionTrigger = 8
	// l0StopWritesTrigger number of files in L0 to stop accepting more writes.
	l0StopWritesTrigger = 24
	// maxBytesForLevelBase base for amount of data stored in a single level.
	maxBytesForLevelBase = 512 * 1024 * 1024
	// cacheSize LRU cache size.
	cacheSize = 8 * 1024 * 1024
	// maxLogFileSize maximum size of WAL files.
	maxLogFileSize = 128 * 1024 * 1024
	// walMinSyncInterval minimum time between calls to WAL file Sync.
	walMinSyncInterval = 500 * time.Microsecond
)

var ErrIsNotDir = errors.New("is not a dir")

func WriterOptions() sstable.WriterOptions {
	return sstable.WriterOptions{
		BlockSize:      blockSize,
		Compression:    sstable.SnappyCompression,
		IndexBlockSize: indexBlockSize,
		FilterPolicy:   bloom.FilterPolicy(10),
		FilterType:     pebble.TableFilter,
	}
}

func syncDir(fs vfs.FS, dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := fs.Stat(dir)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return ErrIsNotDir
	}
	df, err := fs.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		_ = df.Close()
	}()
	return df.Sync()
}

// functions below are used to manage the current data directory of Pebble DB.

// OpenDB opens DB on paths given (using sane defaults).
func OpenDB(fs vfs.FS, dbdir string, walDirname string, cache *pebble.Cache) (*pebble.DB, error) {
	if cache == nil {
		cache = pebble.NewCache(cacheSize)
		defer cache.Unref()
	}

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
		sz = sz * targetFileSizeGrowFactor
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

	return pebble.Open(dbdir, &pebble.Options{
		Cache:                       cache,
		FS:                          fs,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		LBaseMaxBytes:               maxBytesForLevelBase,
		Levels:                      lvlOpts,
		Logger:                      zap.S().Named("pebble"),
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		WALDir:                      walDirname,
		WALMinSyncInterval: func() time.Duration {
			// TODO make interval dynamic based on the load
			return walMinSyncInterval
		},
		EventListener: makeLoggingEventListener(zap.S().Named("pebble").Named("events")),
	})
}

// IsNewRun checks if the SM is created for the first time.
func IsNewRun(fs vfs.FS, dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := fs.Stat(fp); err != nil {
		return true
	}
	return false
}

// GetNodeDBDirName gets DB dir name prefix.
func GetNodeDBDirName(baseDir string, hostname string, name string) string {
	return filepath.Join(baseDir, hostname, name)
}

// GetNewRandomDBDirName gets new random DB dir name.
func GetNewRandomDBDirName() string {
	return fmt.Sprintf("%d_%d", rand.Uint64(), time.Now().UnixNano())
}

// ReplaceCurrentDBFile does currentDBFilename switch (with fsync).
func ReplaceCurrentDBFile(fs vfs.FS, dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := fs.Rename(tmpFp, fp); err != nil {
		return err
	}
	return syncDir(fs, dir)
}

// SaveCurrentDBDirName saves DB name into updatingDBFilename (with fsync).
func SaveCurrentDBDirName(fs vfs.FS, dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}
	fp := filepath.Join(dir, updatingDBFilename)
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = syncDir(fs, dir)
	}()
	if _, err = f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err = f.Write([]byte(dbdir)); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}

// GetCurrentDBDirName reads currentDBFilename file and return its contents.
func GetCurrentDBDirName(fs vfs.FS, dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := fs.Open(fp)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = f.Close()
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		return "", err
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err = h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		return "", err
	}
	return string(content), nil
}

// CreateNodeDataDir creates new SM data dir.
func CreateNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return syncDir(fs, filepath.Dir(dir))
}

// CleanupNodeDataDir cleans up old data dir (should be called after successful switch).
func CleanupNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.RemoveAll(filepath.Join(dir, updatingDBFilename)); err != nil {
		return err
	}
	dbdir, err := GetCurrentDBDirName(fs, dir)
	if err != nil {
		return err
	}
	files, err := fs.List(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if fi == currentDBFilename {
			continue
		}
		toDelete := filepath.Join(dir, fi)
		if toDelete != filepath.Join(dir, dbdir) {
			if err := fs.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}
	return nil
}

func makeLoggingEventListener(logger *zap.SugaredLogger) pebble.EventListener {
	return pebble.EventListener{
		BackgroundError: func(err error) {
			logger.Errorf("background error: %s", err)
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			logger.Debugf("%s", info)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			logger.Infof("%s", info)
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			logger.Warnf("%s", info)
		},
		FlushBegin: func(info pebble.FlushInfo) {
			logger.Debugf("%s", info)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			logger.Debugf("%s", info)
		},
		ManifestCreated: func(info pebble.ManifestCreateInfo) {
			logger.Debugf("%s", info)
		},
		ManifestDeleted: func(info pebble.ManifestDeleteInfo) {
			logger.Debugf("%s", info)
		},
		TableCreated: func(info pebble.TableCreateInfo) {
			logger.Debugf("%s", info)
		},
		TableDeleted: func(info pebble.TableDeleteInfo) {
			logger.Debugf("%s", info)
		},
		TableIngested: func(info pebble.TableIngestInfo) {
			logger.Debugf("%s", info)
		},
		TableStatsLoaded: func(info pebble.TableStatsInfo) {
			logger.Debugf("%s", info)
		},
		WALCreated: func(info pebble.WALCreateInfo) {
			logger.Debugf("%s", info)
		},
		WALDeleted: func(info pebble.WALDeleteInfo) {
			logger.Debugf("%s", info)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			logger.Infof("%s", info)
		},
		WriteStallEnd: func() {
			logger.Infof("write stall ending")
		},
	}
}
