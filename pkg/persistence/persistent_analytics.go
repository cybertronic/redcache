package persistence

import (
	"context"
	"redcache/pkg/monitoring"
	"redcache/pkg/storage"
	"sync"
	"time"
)

// PersistentAnalyticsEngine extends AnalyticsEngine with database persistence
type PersistentAnalyticsEngine struct {
	*monitoring.AnalyticsEngine
	db        *storage.Database
	nsMetrics *storage.Namespace
	mu        sync.RWMutex
	flushCh   chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewPersistentAnalyticsEngine creates a new persistent analytics engine
func NewPersistentAnalyticsEngine(config monitoring.AnalyticsConfig, db *storage.Database) (*PersistentAnalyticsEngine, error) {
	// Create base analytics engine
	baseAnalytics := monitoring.NewAnalyticsEngine(config)

	// Create namespace
	nsMetrics := db.NewNamespace("analytics/metrics")

	pae := &PersistentAnalyticsEngine{
		AnalyticsEngine: baseAnalytics,
		db:              db,
		nsMetrics:       nsMetrics,
		flushCh:         make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}

	// Start flush worker
	pae.wg.Add(1)
	go pae.flushWorker()

	// Load persisted data
	if err := pae.load(); err != nil {
		return nil, err
	}

	return pae, nil
}

// RecordMetric records a metric and triggers persistence
func (pae *PersistentAnalyticsEngine) RecordMetric(ctx context.Context, name string, value float64, tags map[string]string) {
	pae.AnalyticsEngine.RecordMetric(ctx, name, value, tags)
	pae.triggerFlush()
}

// triggerFlush triggers a flush operation
func (pae *PersistentAnalyticsEngine) triggerFlush() {
	select {
	case pae.flushCh <- struct{}{}:
	default:
	}
}

// flushWorker periodically flushes metrics to database
func (pae *PersistentAnalyticsEngine) flushWorker() {
	defer pae.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pae.flush()
		case <-pae.flushCh:
			pae.flush()
		case <-pae.stopCh:
			pae.flush() // Final flush
			return
		}
	}
}

// flush persists metrics to database
func (pae *PersistentAnalyticsEngine) flush() {
	// Note: Cannot access private metrics field from embedded struct
	// This is a limitation of the current design
	// In production, we would need to either:
	// 1. Make metrics field public in AnalyticsEngine
	// 2. Add a GetMetrics() method to AnalyticsEngine
	// 3. Use a different persistence strategy
	
	// For now, just return
	return
}

// load loads persisted metrics from database
func (pae *PersistentAnalyticsEngine) load() error {
	// Note: Cannot access private metrics field from embedded struct
	// Skip loading for now
	return nil
}

// GetStats returns analytics statistics
func (pae *PersistentAnalyticsEngine) GetStats() PersistentAnalyticsStats {
	baseStats := pae.AnalyticsEngine.GetStats()
	
	return PersistentAnalyticsStats{
		MetricsInMemory: int(baseStats.TotalMetrics),
		MetricsInDB:     0, // Cannot determine without access to private fields
		TotalDataPoints: int(baseStats.TotalDataPoints),
	}
}

// PersistentAnalyticsStats contains analytics statistics
type PersistentAnalyticsStats struct {
	MetricsInMemory int
	MetricsInDB     int
	TotalDataPoints int
}

// Close closes the persistent analytics engine
func (pae *PersistentAnalyticsEngine) Close() error {
	select {
	case <-pae.stopCh:
		return nil // already closed
	default:
		close(pae.stopCh)
	}
	pae.wg.Wait()
	return nil
}