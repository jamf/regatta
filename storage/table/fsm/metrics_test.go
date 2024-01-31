// Copyright JAMF Software, LLC

package fsm

import (
	"os"
	"path"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFSM_Metrics(t *testing.T) {
	p := &FSM{
		fs:          vfs.NewMem(),
		clusterID:   1,
		nodeID:      1,
		dirname:     "/tmp",
		log:         zap.NewNop().Sugar(),
		metrics:     newMetrics(testTable, 1),
		appliedFunc: func(applied uint64) {},
	}
	_, _ = p.Open(nil)
	inFile, err := os.Open(path.Join("testdata", "metrics"))
	require.NoError(t, err)
	defer inFile.Close()
	require.NoError(t, err)
	lint, err := testutil.CollectAndLint(p,
		appliedIndexMetricName,
		tableCacheHitsMetricName,
		tableCacheMissesMetricName,
		tableCacheSizeMetricName,
		tableCacheItemsCountMetricName,
		filterHitsMetricName,
		filterMissesMetricName,
		readAmpMetricName,
		writeAmpMetricName,
		bytesInMetricName,
		diskUsageMetricName,
		compactionMetricName,
		compactionDebtMetricName,
	)
	require.NoError(t, err)
	require.Empty(t, lint)
	require.NoError(t, testutil.CollectAndCompare(p, inFile,
		appliedIndexMetricName,
		tableCacheHitsMetricName,
		tableCacheMissesMetricName,
		tableCacheSizeMetricName,
		tableCacheItemsCountMetricName,
		filterHitsMetricName,
		filterMissesMetricName,
		readAmpMetricName,
		writeAmpMetricName,
		bytesInMetricName,
		compactionMetricName,
		compactionDebtMetricName,
	))
}
