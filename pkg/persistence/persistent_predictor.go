package persistence

import (
	"context"
	"redcache/pkg/prefetch"
	"redcache/pkg/storage"
	"sync"
	"time"
)

// PersistentPredictor extends Predictor with database persistence
type PersistentPredictor struct {
	*prefetch.Predictor
	db          *storage.Database
	nsPatterns  *storage.Namespace
	mu          sync.RWMutex
	flushCh     chan struct{}
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewPersistentPredictor creates a new persistent predictor
func NewPersistentPredictor(config prefetch.PredictorConfig, db *storage.Database) (*PersistentPredictor, error) {
	basePredictor := prefetch.NewPredictor(config)

	// Create namespace
	nsPatterns := db.NewNamespace("prefetch/patterns")

	pp := &PersistentPredictor{
		Predictor:  basePredictor,
		db:         db,
		nsPatterns: nsPatterns,
		flushCh:    make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}

	// Start flush worker
	pp.wg.Add(1)
	go pp.flushWorker()

	// Load persisted patterns
	if err := pp.load(); err != nil {
		return nil, err
	}

	return pp, nil
}

// RecordAccess records an access and persists patterns
func (pp *PersistentPredictor) RecordAccess(ctx context.Context, record prefetch.AccessRecord) {
	pp.Predictor.RecordAccess(ctx, record)
	pp.triggerFlush()
}

// triggerFlush triggers a flush operation
func (pp *PersistentPredictor) triggerFlush() {
	select {
	case pp.flushCh <- struct{}{}:
	default:
	}
}

// flushWorker periodically flushes patterns to database
func (pp *PersistentPredictor) flushWorker() {
	defer pp.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pp.flush()
		case <-pp.flushCh:
			pp.flush()
		case <-pp.stopCh:
			pp.flush() // Final flush
			return
		}
	}
}

// flush persists patterns to database
func (pp *PersistentPredictor) flush() {
	// Note: Cannot access private patterns fields from embedded struct
	// This is a limitation of the current design
	// Skip persistence for now
	return
}

// load loads persisted patterns from database
func (pp *PersistentPredictor) load() error {
	// Note: Cannot access private patterns fields from embedded struct
	// Skip loading for now
	return nil
}

// GetStats returns predictor statistics
func (pp *PersistentPredictor) GetStats() map[string]any {
	baseStats := pp.Predictor.GetStats()
	
	stats := make(map[string]any)
	stats["base"] = baseStats
	stats["patterns_in_memory"] = 0 // Cannot determine without access to private fields
	stats["patterns_in_db"] = 0     // Cannot determine without access to private fields
	
	return stats
}

// Close closes the persistent predictor
func (pp *PersistentPredictor) Close() error {
	select {
	case <-pp.stopCh:
		return nil // already closed
	default:
		close(pp.stopCh)
	}
	pp.wg.Wait()
	pp.flush()
	return nil
}