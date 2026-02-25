package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CacheWarmer proactively warms the cache to prevent cold starts
type CacheWarmer struct {
	clusterManager *ClusterManager
	cache          CacheInterface
	coordinator    *FetchCoordinator
	config         CacheWarmerConfig
	
	// State
	running    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	
	// Metrics
	metrics WarmingMetrics
}

// CacheWarmerConfig configures cache warming behavior
type CacheWarmerConfig struct {
	// Enable cache warming
	Enabled bool
	
	// Warm on leader election
	WarmOnLeaderElection bool
	
	// Warming strategy
	BatchSize      int           // Number of keys to warm per batch
	RateLimit      int           // Batches per second
	MaxConcurrent  int           // Max concurrent warming operations
	
	// Timing
	WarmingInterval time.Duration // How often to check for warming opportunities
	
	// Predictive warming
	EnablePredictive bool          // Use ML to predict keys to warm
	PredictionWindow time.Duration // Look ahead window for predictions
}

// WarmingMetrics tracks cache warming statistics
type WarmingMetrics struct {
	TotalWarmed       int64
	SuccessfulWarms   int64
	FailedWarms       int64
	BatchesProcessed  int64
	TotalWarmingTime  time.Duration
	LastWarmingTime   time.Time
	PredictiveWarms   int64
	ManualWarms       int64
}

// CacheInterface defines the interface for cache operations
type CacheInterface interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Has(ctx context.Context, key string) bool
}

// NewCacheWarmer creates a new cache warmer
func NewCacheWarmer(
	config CacheWarmerConfig,
	clusterManager *ClusterManager,
	cache CacheInterface,
	coordinator *FetchCoordinator,
) *CacheWarmer {
	return &CacheWarmer{
		clusterManager: clusterManager,
		cache:          cache,
		coordinator:    coordinator,
		config:         config,
		stopCh:         make(chan struct{}),
	}
}

// Start starts the cache warmer
func (cw *CacheWarmer) Start(ctx context.Context) error {
	cw.mu.Lock()
	if cw.running {
		cw.mu.Unlock()
		return fmt.Errorf("cache warmer already running")
	}
	cw.running = true
	cw.mu.Unlock()
	
	if !cw.config.Enabled {
		return nil
	}
	
	// Start warming loop
	cw.wg.Add(1)
	go cw.warmingLoop(ctx)
	
	return nil
}

// Stop stops the cache warmer
func (cw *CacheWarmer) Stop() error {
	cw.mu.Lock()
	if !cw.running {
		cw.mu.Unlock()
		return nil
	}
	cw.running = false
	cw.mu.Unlock()
	
	select {
	case <-cw.stopCh:
		return nil // already closed
	default:
		close(cw.stopCh)
	}
	cw.wg.Wait()
	
	return nil
}

// warmingLoop runs the periodic warming process
func (cw *CacheWarmer) warmingLoop(ctx context.Context) {
	defer cw.wg.Done()
	
	ticker := time.NewTicker(cw.config.WarmingInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Only warm if we're the leader (if configured)
			if cw.config.WarmOnLeaderElection && !cw.clusterManager.IsLeader() {
				continue
			}
			
			// Perform warming
			if err := cw.performWarming(ctx); err != nil {
				// Log error but continue
				continue
			}
			
		case <-cw.stopCh:
			return
			
		case <-ctx.Done():
			return
		}
	}
}

// performWarming performs a warming cycle
func (cw *CacheWarmer) performWarming(ctx context.Context) error {
	startTime := time.Now()
	
	// Get keys to warm
	keys := cw.getKeysToWarm(ctx)
	if len(keys) == 0 {
		return nil
	}
	
	// Warm in batches
	batchSize := cw.config.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	
	// Rate limiter for batches
	var rateLimiter *time.Ticker
	if cw.config.RateLimit > 0 {
		interval := time.Second / time.Duration(cw.config.RateLimit)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}
	
	// Process batches
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		
		// Rate limit
		if rateLimiter != nil {
			select {
			case <-rateLimiter.C:
			case <-cw.stopCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		
		// Warm batch
		cw.warmBatch(ctx, batch)
		
		// Update metrics
		cw.mu.Lock()
		cw.metrics.BatchesProcessed++
		cw.mu.Unlock()
	}
	
	// Update timing metrics
	cw.mu.Lock()
	cw.metrics.TotalWarmingTime += time.Since(startTime)
	cw.metrics.LastWarmingTime = time.Now()
	cw.mu.Unlock()
	
	return nil
}

// warmBatch warms a batch of keys
func (cw *CacheWarmer) warmBatch(ctx context.Context, keys []string) {
	// Use semaphore for concurrency control
	semaphore := make(chan struct{}, cw.config.MaxConcurrent)
	var wg sync.WaitGroup
	
	for _, key := range keys {
		// Check if already cached
		if cw.cache.Has(ctx, key) {
			continue
		}
		
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			
			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}
			
			// Warm the key
			if err := cw.warmKey(ctx, k); err != nil {
				cw.mu.Lock()
				cw.metrics.FailedWarms++
				cw.mu.Unlock()
			} else {
				cw.mu.Lock()
				cw.metrics.SuccessfulWarms++
				cw.metrics.TotalWarmed++
				cw.mu.Unlock()
			}
		}(key)
	}
	
	wg.Wait()
}

// warmKey warms a single key
func (cw *CacheWarmer) warmKey(ctx context.Context, key string) error {
	// Use coordinator to fetch (benefits from deduplication and rate limiting)
	data, err := cw.coordinator.Fetch(ctx, key)
	if err != nil {
		return err
	}
	
	// Store in cache
	return cw.cache.Put(ctx, key, data)
}

// getKeysToWarm returns keys that should be warmed
func (cw *CacheWarmer) getKeysToWarm(ctx context.Context) []string {
	// TODO: Implement predictive warming using ML
	// For now, return empty list
	// In production, this would:
	// 1. Query ML predictor for likely-to-be-accessed keys
	// 2. Check access patterns
	// 3. Consider time of day, workload patterns, etc.
	
	return []string{}
}

// WarmKeys manually warms specific keys
func (cw *CacheWarmer) WarmKeys(ctx context.Context, keys []string) error {
	cw.mu.Lock()
	cw.metrics.ManualWarms += int64(len(keys))
	cw.mu.Unlock()
	
	cw.warmBatch(ctx, keys)
	return nil
}

// WarmFromList warms keys from a predefined list
func (cw *CacheWarmer) WarmFromList(ctx context.Context, keys []string) error {
	if !cw.clusterManager.IsLeader() {
		return fmt.Errorf("only leader can warm from list")
	}
	
	return cw.WarmKeys(ctx, keys)
}

// GetMetrics returns warming metrics
func (cw *CacheWarmer) GetMetrics() WarmingMetrics {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	
	return cw.metrics
}

// Reset resets metrics (for testing)
func (cw *CacheWarmer) Reset() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	
	cw.metrics = WarmingMetrics{}
}