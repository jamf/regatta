// Copyright JAMF Software, LLC

package fsm

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	cacheHits               *prometheus.GaugeVec
	cacheMisses             *prometheus.GaugeVec
	cacheSize               *prometheus.GaugeVec
	cacheCount              *prometheus.GaugeVec
	filterHits              prometheus.Gauge
	filterMisses            prometheus.Gauge
	diskSpaceUsageBytes     prometheus.Gauge
	readAmplification       prometheus.Gauge
	totalWriteAmplification prometheus.Gauge
	totalBytesIn            prometheus.Gauge
	compactCount            *prometheus.GaugeVec
	compactDebt             prometheus.Gauge
	collected               *pebble.Metrics
}

func newMetrics(tableName string, clusterID uint64) *metrics {
	return &metrics{
		cacheHits: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_cache_hits",
				Help: "Regatta table storage block/table cache hits",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			}, []string{"type"},
		),
		cacheMisses: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_cache_misses",
				Help: "Regatta table storage block/table cache misses",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			}, []string{"type"},
		),
		cacheSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_cache_size_bytes",
				Help: "Regatta table storage block/table cache size in bytes",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			}, []string{"type"},
		),
		cacheCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_cache_count",
				Help: "Regatta table storage block/table cache items count",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			}, []string{"type"},
		),
		filterHits: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_filter_hits",
				Help: "Regatta table storage bloom filter hits",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		filterMisses: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_filter_misses",
				Help: "Regatta table storage bloom filter misses",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		diskSpaceUsageBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_disk_usage_bytes",
				Help: "Regatta table storage estimated disk usage, including temp files and WAL",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		readAmplification: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_read_amp",
				Help: "Regatta table storage read amplification",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		totalWriteAmplification: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_total_write_amp",
				Help: "Regatta table storage total write amplification",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		totalBytesIn: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_total_bytes_in",
				Help: "Regatta table storage total bytes in",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
		compactCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_compaction_count",
				Help: "Regatta table storage compaction count by kind",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			}, []string{"kind"},
		),
		compactDebt: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "regatta_table_storage_compaction_debt_bytes",
				Help: "Regatta table storage compaction debt in bytes",
				ConstLabels: map[string]string{
					"table":     tableName,
					"clusterID": fmt.Sprintf("%d", clusterID),
				},
			},
		),
	}
}

func (p *metrics) Collect(ch chan<- prometheus.Metric) {
	p.cacheHits.With(prometheus.Labels{"type": "block"}).Set(float64(p.collected.BlockCache.Hits))
	p.cacheMisses.With(prometheus.Labels{"type": "block"}).Set(float64(p.collected.BlockCache.Misses))
	p.cacheSize.With(prometheus.Labels{"type": "block"}).Set(float64(p.collected.BlockCache.Size))
	p.cacheCount.With(prometheus.Labels{"type": "block"}).Set(float64(p.collected.BlockCache.Count))
	p.cacheHits.With(prometheus.Labels{"type": "table"}).Set(float64(p.collected.TableCache.Hits))
	p.cacheMisses.With(prometheus.Labels{"type": "table"}).Set(float64(p.collected.TableCache.Misses))
	p.cacheSize.With(prometheus.Labels{"type": "table"}).Set(float64(p.collected.TableCache.Size))
	p.cacheCount.With(prometheus.Labels{"type": "table"}).Set(float64(p.collected.TableCache.Count))
	p.cacheHits.Collect(ch)
	p.cacheMisses.Collect(ch)
	p.cacheSize.Collect(ch)
	p.cacheCount.Collect(ch)

	p.filterHits.Set(float64(p.collected.Filter.Hits))
	p.filterMisses.Set(float64(p.collected.Filter.Misses))
	p.filterHits.Collect(ch)
	p.filterMisses.Collect(ch)

	p.diskSpaceUsageBytes.Set(float64(p.collected.DiskSpaceUsage()))
	p.diskSpaceUsageBytes.Collect(ch)

	p.readAmplification.Set(float64(p.collected.ReadAmp()))
	p.readAmplification.Collect(ch)

	total := p.collected.Total()
	p.totalWriteAmplification.Set(total.WriteAmp())
	p.totalWriteAmplification.Collect(ch)
	p.totalBytesIn.Set(float64(total.BytesIn))
	p.totalBytesIn.Collect(ch)

	compact := p.collected.Compact
	p.compactCount.With(prometheus.Labels{"kind": "total"}).Set(float64(compact.Count))
	p.compactCount.With(prometheus.Labels{"kind": "default"}).Set(float64(compact.DefaultCount))
	p.compactCount.With(prometheus.Labels{"kind": "delete"}).Set(float64(compact.DeleteOnlyCount))
	p.compactCount.With(prometheus.Labels{"kind": "elision"}).Set(float64(compact.ElisionOnlyCount))
	p.compactCount.With(prometheus.Labels{"kind": "move"}).Set(float64(compact.MoveCount))
	p.compactCount.With(prometheus.Labels{"kind": "read"}).Set(float64(compact.ReadCount))
	p.compactCount.With(prometheus.Labels{"kind": "multilevel"}).Set(float64(compact.MultiLevelCount))
	p.compactCount.Collect(ch)

	p.compactDebt.Set(float64(compact.EstimatedDebt))
	p.compactDebt.Collect(ch)
}

func (p *metrics) Describe(ch chan<- *prometheus.Desc) {
	p.cacheHits.Describe(ch)
	p.cacheMisses.Describe(ch)
	p.cacheSize.Describe(ch)
	p.cacheCount.Describe(ch)
	p.filterHits.Describe(ch)
	p.filterMisses.Describe(ch)
	p.diskSpaceUsageBytes.Describe(ch)
	p.readAmplification.Describe(ch)
	p.totalWriteAmplification.Describe(ch)
	p.totalBytesIn.Describe(ch)
	p.compactCount.Describe(ch)
	p.compactDebt.Describe(ch)
}
