package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector collects and exposes metrics
type Collector struct {
	namespace string

	// Cache metrics
	CacheSize         prometheus.Gauge
	CacheEntries      prometheus.Gauge
	CacheHitRate      prometheus.Gauge
	CacheCapacityUsed prometheus.Gauge
	CacheHits         prometheus.Counter
	CacheMisses       prometheus.Counter
	CacheEvictions    prometheus.Counter

	// Request metrics
	RequestsTotal    prometheus.Counter
	RequestDuration  prometheus.Histogram
	RequestSize      prometheus.Histogram
	ResponseSize     prometheus.Histogram

	// S3 metrics
	S3RequestsTotal  prometheus.Counter
	S3RequestDuration prometheus.Histogram
	S3Errors         prometheus.Counter

	// Prefetch metrics
	PrefetchAccuracy    prometheus.Gauge
	PrefetchPredictions prometheus.Counter
	PrefetchHits        prometheus.Counter

	// Compression metrics
	CompressionRatio     prometheus.Gauge
	CompressionBytesSaved prometheus.Counter

	// Deduplication metrics
	DeduplicationRatio     prometheus.Gauge
	DeduplicationBytesSaved prometheus.Counter
}

// NewCollector creates a new metrics collector
func NewCollector(namespace string) *Collector {
	c := &Collector{
		namespace: namespace,
	}

	// Initialize cache metrics
	c.CacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cache_size_bytes",
		Help:      "Current cache size in bytes",
	})

	c.CacheEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cache_entries",
		Help:      "Number of entries in cache",
	})

	c.CacheHitRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cache_hit_rate",
		Help:      "Cache hit rate (0-1)",
	})

	c.CacheCapacityUsed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cache_capacity_used",
		Help:      "Cache capacity used (0-1)",
	})

	c.CacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "cache_hits_total",
		Help:      "Total number of cache hits",
	})

	c.CacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "cache_misses_total",
		Help:      "Total number of cache misses",
	})

	c.CacheEvictions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "cache_evictions_total",
		Help:      "Total number of cache evictions",
	})

	// Initialize request metrics
	c.RequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "requests_total",
		Help:      "Total number of requests",
	})

	c.RequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "request_duration_seconds",
		Help:      "Request duration in seconds",
		Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5},
	})

	c.RequestSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "request_size_bytes",
		Help:      "Request size in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 10),
	})

	c.ResponseSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "response_size_bytes",
		Help:      "Response size in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 10),
	})

	// Initialize S3 metrics
	c.S3RequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "s3_requests_total",
		Help:      "Total number of S3 requests",
	})

	c.S3RequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "s3_request_duration_seconds",
		Help:      "S3 request duration in seconds",
		Buckets:   []float64{.01, .05, .1, .5, 1, 5, 10},
	})

	c.S3Errors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "s3_errors_total",
		Help:      "Total number of S3 errors",
	})

	// Initialize prefetch metrics
	c.PrefetchAccuracy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "prefetch_accuracy",
		Help:      "Prefetch prediction accuracy (0-1)",
	})

	c.PrefetchPredictions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "prefetch_predictions_total",
		Help:      "Total number of prefetch predictions",
	})

	c.PrefetchHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "prefetch_hits_total",
		Help:      "Total number of successful prefetch predictions",
	})

	// Initialize compression metrics
	c.CompressionRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "compression_ratio",
		Help:      "Average compression ratio",
	})

	c.CompressionBytesSaved = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "compression_bytes_saved_total",
		Help:      "Total bytes saved through compression",
	})

	// Initialize deduplication metrics
	c.DeduplicationRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "deduplication_ratio",
		Help:      "Deduplication ratio",
	})

	c.DeduplicationBytesSaved = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "deduplication_bytes_saved_total",
		Help:      "Total bytes saved through deduplication",
	})

	// FIX #30: use mustRegisterOnce instead of prometheus.MustRegister.
	// MustRegister panics when the same metric is registered twice (e.g. in
	// tests that call NewCollector more than once).  mustRegisterOnce silently
	// reuses the already-registered collector instead of panicking.
	mustRegisterOnce(
		c.CacheSize,
		c.CacheEntries,
		c.CacheHitRate,
		c.CacheCapacityUsed,
		c.CacheHits,
		c.CacheMisses,
		c.CacheEvictions,
		c.RequestsTotal,
		c.RequestDuration,
		c.RequestSize,
		c.ResponseSize,
		c.S3RequestsTotal,
		c.S3RequestDuration,
		c.S3Errors,
		c.PrefetchAccuracy,
		c.PrefetchPredictions,
		c.PrefetchHits,
		c.CompressionRatio,
		c.CompressionBytesSaved,
		c.DeduplicationRatio,
		c.DeduplicationBytesSaved,
	)

	return c
}

// mustRegisterOnce registers each collector with the default Prometheus
// registry.  If a collector with the same descriptor is already registered,
// the error is silently ignored so that calling NewCollector multiple times
// (e.g. in tests) does not panic.
func mustRegisterOnce(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := prometheus.Register(c); err != nil {
			// AlreadyRegisteredError means the metric was registered before;
			// this is safe to ignore — the existing registration is reused.
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				// Any other error is unexpected — panic to surface it.
				panic(err)
			}
		}
	}
}

// Server serves Prometheus metrics
type Server struct {
	httpServer *http.Server
	collector  *Collector
}

// NewServer creates a new metrics server
func NewServer(addr string, collector *Collector) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		collector: collector,
	}
}

// Start starts the metrics server
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the metrics server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// RecordCacheHit records a cache hit
func (c *Collector) RecordCacheHit() {
	c.CacheHits.Inc()
}

// RecordCacheMiss records a cache miss
func (c *Collector) RecordCacheMiss() {
	c.CacheMisses.Inc()
}

// RecordCacheEviction records a cache eviction
func (c *Collector) RecordCacheEviction() {
	c.CacheEvictions.Inc()
}

// RecordRequest records a request with duration
func (c *Collector) RecordRequest(duration time.Duration, requestSize, responseSize int64) {
	c.RequestsTotal.Inc()
	c.RequestDuration.Observe(duration.Seconds())
	c.RequestSize.Observe(float64(requestSize))
	c.ResponseSize.Observe(float64(responseSize))
}

// RecordS3Request records an S3 request
func (c *Collector) RecordS3Request(duration time.Duration, err error) {
	c.S3RequestsTotal.Inc()
	c.S3RequestDuration.Observe(duration.Seconds())
	if err != nil {
		c.S3Errors.Inc()
	}
}

// UpdateCacheStats updates cache statistics
func (c *Collector) UpdateCacheStats(size, maxSize int64, entries int, hitRate float64) {
	c.CacheSize.Set(float64(size))
	c.CacheEntries.Set(float64(entries))
	c.CacheHitRate.Set(hitRate)
	if maxSize > 0 {
		c.CacheCapacityUsed.Set(float64(size) / float64(maxSize))
	}
}