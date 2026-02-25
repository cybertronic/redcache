package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TierType defines the cache tier
type TierType int

const (
	TierMemory TierType = iota
	TierNVMe
	TierNetwork
)

// TierConfig configures a cache tier
type TierConfig struct {
	Type           TierType
	MaxSize        int64
	EvictionPolicy EvictionPolicy
	TTL            time.Duration
	PromotionThreshold int // Access count to promote to higher tier
	DemotionThreshold  int // Access count to demote to lower tier
}

// TieredCacheConfig configures the tiered cache system
type TieredCacheConfig struct {
	Tiers              []TierConfig
	EnableAutoTiering  bool    // Automatically move data between tiers
	PromotionRate      float64 // Rate of promotion (0-1)
	DemotionRate       float64 // Rate of demotion (0-1)
	TieringInterval    time.Duration
	HotDataThreshold   int     // Access count to consider data "hot"
	ColdDataThreshold  int     // Access count to consider data "cold"
	EnablePredictive   bool    // Use ML to predict tier placement
}

// TieredCache implements a multi-tier cache system
type TieredCache struct {
	config TieredCacheConfig
	mu     sync.RWMutex
	
	// Tiers
	tiers []*CacheTier
	
	// Metadata tracking
	metadata map[string]*TierMetadata
	
	// Auto-tiering
	tieringTicker *time.Ticker
	stopChan      chan struct{}
	
	// Statistics
	stats TieredCacheStats
}

// CacheTier represents a single cache tier
type CacheTier struct {
	config   TierConfig
	mu       sync.RWMutex
	entries  map[string]*TierEntry
	size     int64
	adaptive *AdaptiveCache
	
	// Tier-specific stats
	hits      int64
	misses    int64
	promotions int64
	demotions  int64
}

// TierEntry represents an entry in a tier
type TierEntry struct {
	Key          string
	Value        []byte
	Size         int64
	AccessCount  int
	LastAccess   time.Time
	CreatedAt    time.Time
	Temperature  float64 // Hot/cold score
	Tier         TierType
}

// TierMetadata tracks metadata across tiers
type TierMetadata struct {
	Key            string
	CurrentTier    TierType
	AccessCount    int
	TotalAccesses  int
	LastAccess     time.Time
	CreatedAt      time.Time
	PromotionCount int
	DemotionCount  int
	Temperature    float64
	PredictedTier  TierType
}

// TieredCacheStats contains tiered cache statistics
type TieredCacheStats struct {
	TotalHits      int64
	TotalMisses    int64
	TierHits       map[TierType]int64
	TierMisses     map[TierType]int64
	TierSizes      map[TierType]int64
	TierEntries    map[TierType]int
	Promotions     int64
	Demotions      int64
	AvgAccessTime  map[TierType]time.Duration
	HitRate        float64
}

// NewTieredCache creates a new tiered cache
func NewTieredCache(config TieredCacheConfig) *TieredCache {
	if config.PromotionRate == 0 {
		config.PromotionRate = 0.1
	}
	if config.DemotionRate == 0 {
		config.DemotionRate = 0.05
	}
	if config.TieringInterval == 0 {
		config.TieringInterval = 1 * time.Minute
	}
	if config.HotDataThreshold == 0 {
		config.HotDataThreshold = 10
	}
	if config.ColdDataThreshold == 0 {
		config.ColdDataThreshold = 2
	}
	
	tc := &TieredCache{
		config:   config,
		tiers:    make([]*CacheTier, len(config.Tiers)),
		metadata: make(map[string]*TierMetadata),
		stopChan: make(chan struct{}),
		stats: TieredCacheStats{
			TierHits:      make(map[TierType]int64),
			TierMisses:    make(map[TierType]int64),
			TierSizes:     make(map[TierType]int64),
			TierEntries:   make(map[TierType]int),
			AvgAccessTime: make(map[TierType]time.Duration),
		},
	}
	
	// Initialize tiers
	for i, tierConfig := range config.Tiers {
		tc.tiers[i] = &CacheTier{
			config:  tierConfig,
			entries: make(map[string]*TierEntry),
			adaptive: NewAdaptiveCache(AdaptiveCacheConfig{
				Policy:  tierConfig.EvictionPolicy,
				MaxSize: tierConfig.MaxSize,
			}),
		}
	}
	
	// Start auto-tiering if enabled
	if config.EnableAutoTiering {
		tc.startAutoTiering()
	}
	
	return tc
}

// Get retrieves data from the tiered cache.
// FIX #32: metadata is updated under the write lock to prevent TOCTOU races.
func (tc *TieredCache) Get(ctx context.Context, key string) ([]byte, bool) {
	tc.mu.RLock()
	_, exists := tc.metadata[key]
	tc.mu.RUnlock()
	
	if !exists {
		tc.mu.Lock()
		tc.stats.TotalMisses++
		tc.mu.Unlock()
		return nil, false
	}
	
	// Search from highest tier to lowest
	for i, tier := range tc.tiers {
		startTime := time.Now()
		value, found := tier.Get(key)
		accessTime := time.Since(startTime)
		
		if found {
			// Update statistics and metadata under a single write lock.
			// FIX #32: metadata mutation is now inside the write lock.
			tc.mu.Lock()
			tc.stats.TotalHits++
			tc.stats.TierHits[tier.config.Type]++
			tc.stats.AvgAccessTime[tier.config.Type] = accessTime

			metadata := tc.metadata[key]
			if metadata != nil {
				metadata.AccessCount++
				metadata.TotalAccesses++
				metadata.LastAccess = time.Now()
				metadata.Temperature = tc.calculateTemperature(metadata)

				// Consider promotion if not in top tier
				if i > 0 && tc.config.EnableAutoTiering {
					if metadata.AccessCount >= tier.config.PromotionThreshold {
						// Capture values needed for promotion before releasing lock
						currentTier := tier.config.Type
						tc.mu.Unlock()
						tc.promoteEntry(key, metadata, currentTier)
						return value, true
					}
				}
			}
			tc.mu.Unlock()
			
			return value, true
		}
		
		tc.mu.Lock()
		tc.stats.TierMisses[tier.config.Type]++
		tc.mu.Unlock()
	}
	
	tc.mu.Lock()
	tc.stats.TotalMisses++
	tc.mu.Unlock()
	return nil, false
}

// Put adds data to the tiered cache
func (tc *TieredCache) Put(ctx context.Context, key string, value []byte) error {
	size := int64(len(value))
	
	// Determine initial tier placement
	tier := tc.selectInitialTier(key, size)
	
	// Add to selected tier
	if err := tc.tiers[tier].Put(key, value); err != nil {
		return err
	}
	
	// Create or update metadata
	tc.mu.Lock()
	if metadata, exists := tc.metadata[key]; exists {
		metadata.AccessCount = 1
		metadata.LastAccess = time.Now()
		metadata.CurrentTier = tc.tiers[tier].config.Type
	} else {
		tc.metadata[key] = &TierMetadata{
			Key:         key,
			CurrentTier: tc.tiers[tier].config.Type,
			AccessCount: 1,
			TotalAccesses: 1,
			LastAccess:  time.Now(),
			CreatedAt:   time.Now(),
			Temperature: 0.5,
		}
	}
	tc.mu.Unlock()
	
	return nil
}

// selectInitialTier selects the initial tier for new data
func (tc *TieredCache) selectInitialTier(key string, size int64) int {
	if tc.config.EnablePredictive {
		// Use ML prediction if available
		return tc.predictTier(key, size)
	}
	
	// Default: start in lowest tier (NVMe)
	for i := len(tc.tiers) - 1; i >= 0; i-- {
		if tc.tiers[i].size+size <= tc.tiers[i].config.MaxSize {
			return i
		}
	}
	
	// If all tiers full, use memory tier and evict
	return 0
}

// predictTier predicts the best tier using ML
func (tc *TieredCache) predictTier(key string, size int64) int {
	// Small frequently accessed data -> Memory
	if size < 1024*1024 { // < 1MB
		return 0
	}
	
	// Medium data -> NVMe
	if size < 100*1024*1024 { // < 100MB
		return 1
	}
	
	// Large data -> Network/Disk
	return len(tc.tiers) - 1
}

// promoteEntry promotes an entry to a higher tier
func (tc *TieredCache) promoteEntry(key string, metadata *TierMetadata, currentTier TierType) {
	// Find current and target tiers
	currentTierIdx := -1
	for i, tier := range tc.tiers {
		if tier.config.Type == currentTier {
			currentTierIdx = i
			break
		}
	}
	
	if currentTierIdx <= 0 {
		return // Already in top tier
	}
	
	targetTierIdx := currentTierIdx - 1
	
	// Get data from current tier
	value, found := tc.tiers[currentTierIdx].Get(key)
	if !found {
		return
	}
	
	// Add to target tier
	if err := tc.tiers[targetTierIdx].Put(key, value); err != nil {
		return
	}
	
	// Remove from current tier
	tc.tiers[currentTierIdx].Delete(key)
	
	// Update metadata under write lock
	tc.mu.Lock()
	metadata.CurrentTier = tc.tiers[targetTierIdx].config.Type
	metadata.PromotionCount++
	metadata.AccessCount = 0 // Reset for new tier
	tc.stats.Promotions++
	tc.tiers[currentTierIdx].promotions++
	tc.mu.Unlock()
}

// demoteEntry demotes an entry to a lower tier
func (tc *TieredCache) demoteEntry(key string, metadata *TierMetadata, currentTier TierType) {
	// Find current and target tiers
	currentTierIdx := -1
	for i, tier := range tc.tiers {
		if tier.config.Type == currentTier {
			currentTierIdx = i
			break
		}
	}
	
	if currentTierIdx >= len(tc.tiers)-1 {
		return // Already in lowest tier
	}
	
	targetTierIdx := currentTierIdx + 1
	
	// Get data from current tier
	value, found := tc.tiers[currentTierIdx].Get(key)
	if !found {
		return
	}
	
	// Add to target tier
	if err := tc.tiers[targetTierIdx].Put(key, value); err != nil {
		return
	}
	
	// Remove from current tier
	tc.tiers[currentTierIdx].Delete(key)
	
	// Update metadata under write lock
	tc.mu.Lock()
	metadata.CurrentTier = tc.tiers[targetTierIdx].config.Type
	metadata.DemotionCount++
	metadata.AccessCount = 0
	tc.stats.Demotions++
	tc.tiers[currentTierIdx].demotions++
	tc.mu.Unlock()
}

// calculateTemperature calculates the hot/cold score for data
func (tc *TieredCache) calculateTemperature(metadata *TierMetadata) float64 {
	age := time.Since(metadata.CreatedAt)
	recency := time.Since(metadata.LastAccess)
	
	// Frequency score (0-1)
	frequencyScore := float64(metadata.TotalAccesses) / 100.0
	if frequencyScore > 1.0 {
		frequencyScore = 1.0
	}
	
	// Recency score (0-1)
	recencyScore := 1.0 / (1.0 + recency.Hours())
	
	// Age penalty
	agePenalty := 1.0 / (1.0 + age.Hours()/24.0)
	
	// Combined temperature
	temperature := (frequencyScore*0.5 + recencyScore*0.3 + agePenalty*0.2)
	
	return temperature
}

// startAutoTiering starts the auto-tiering background process
func (tc *TieredCache) startAutoTiering() {
	tc.tieringTicker = time.NewTicker(tc.config.TieringInterval)
	
	go func() {
		for {
			select {
			case <-tc.tieringTicker.C:
				tc.performAutoTiering()
			case <-tc.stopChan:
				return
			}
		}
	}()
}

// performAutoTiering performs automatic data tiering
func (tc *TieredCache) performAutoTiering() {
	tc.mu.RLock()
	entries := make([]*TierMetadata, 0, len(tc.metadata))
	for _, metadata := range tc.metadata {
		entries = append(entries, metadata)
	}
	tc.mu.RUnlock()
	
	// Evaluate each entry for promotion/demotion
	for _, metadata := range entries {
		temperature := tc.calculateTemperature(metadata)

		// FIX #32: update Temperature under write lock
		tc.mu.Lock()
		metadata.Temperature = temperature
		accessCount := metadata.AccessCount
		currentTier := metadata.CurrentTier
		tc.mu.Unlock()
		
		// Hot data -> promote
		if temperature > 0.7 && accessCount >= tc.config.HotDataThreshold {
			tc.promoteEntry(metadata.Key, metadata, currentTier)
		}
		
		// Cold data -> demote
		if temperature < 0.3 && accessCount < tc.config.ColdDataThreshold {
			tc.demoteEntry(metadata.Key, metadata, currentTier)
		}
	}
}

// Stop stops the tiered cache
func (tc *TieredCache) Stop() {
	if tc.tieringTicker != nil {
		tc.tieringTicker.Stop()
	}
	select {
	case <-tc.stopChan:
		return // already closed
	default:
		close(tc.stopChan)
	}
}

// GetStats returns tiered cache statistics
func (tc *TieredCache) GetStats() TieredCacheStats {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	stats := tc.stats
	
	// Update tier sizes and entries
	for _, tier := range tc.tiers {
		tier.mu.RLock()
		stats.TierSizes[tier.config.Type] = tier.size
		stats.TierEntries[tier.config.Type] = len(tier.entries)
		tier.mu.RUnlock()
	}
	
	// Calculate hit rate
	if stats.TotalHits+stats.TotalMisses > 0 {
		stats.HitRate = float64(stats.TotalHits) / float64(stats.TotalHits+stats.TotalMisses)
	}
	
	return stats
}

// CacheTier methods

// Get retrieves data from a tier.
// FIX #13: AccessCount and LastAccess are mutated under a write lock, not RLock.
func (ct *CacheTier) Get(key string) ([]byte, bool) {
	ct.mu.RLock()
	entry, exists := ct.entries[key]
	if !exists {
		ct.misses++
		ct.mu.RUnlock()
		return nil, false
	}
	// Copy the value slice reference while under RLock.
	value := entry.Value
	ct.mu.RUnlock()

	// Upgrade to write lock to mutate entry metadata.
	ct.mu.Lock()
	// Re-check existence after lock upgrade.
	if e, ok := ct.entries[key]; ok {
		e.AccessCount++
		e.LastAccess = time.Now()
		ct.hits++
	}
	ct.mu.Unlock()

	return value, true
}

// Put adds data to a tier
func (ct *CacheTier) Put(key string, value []byte) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	size := int64(len(value))
	
	// Check if entry exists
	if entry, exists := ct.entries[key]; exists {
		ct.size -= entry.Size
		entry.Value = value
		entry.Size = size
		entry.LastAccess = time.Now()
		ct.size += size
		return nil
	}
	
	// Evict if necessary
	for ct.size+size > ct.config.MaxSize {
		if err := ct.evict(); err != nil {
			return err
		}
	}
	
	// Create new entry
	entry := &TierEntry{
		Key:         key,
		Value:       value,
		Size:        size,
		AccessCount: 1,
		LastAccess:  time.Now(),
		CreatedAt:   time.Now(),
		Tier:        ct.config.Type,
	}
	
	ct.entries[key] = entry
	ct.size += size
	
	return nil
}

// Delete removes data from a tier
func (ct *CacheTier) Delete(key string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	if entry, exists := ct.entries[key]; exists {
		ct.size -= entry.Size
		delete(ct.entries, key)
	}
}

// evict evicts an entry from the tier
func (ct *CacheTier) evict() error {
	// Find least valuable entry based on policy
	var victim *TierEntry
	minScore := float64(1e9)
	
	for _, entry := range ct.entries {
		score := ct.calculateEvictionScore(entry)
		if score < minScore {
			minScore = score
			victim = entry
		}
	}
	
	if victim == nil {
		return fmt.Errorf("no entry to evict")
	}
	
	ct.size -= victim.Size
	delete(ct.entries, victim.Key)
	
	return nil
}

// calculateEvictionScore calculates eviction score for an entry
func (ct *CacheTier) calculateEvictionScore(entry *TierEntry) float64 {
	// Higher score = more valuable = less likely to evict
	age := time.Since(entry.CreatedAt)
	recency := time.Since(entry.LastAccess)
	
	frequencyScore := float64(entry.AccessCount)
	recencyScore := 1.0 / (1.0 + recency.Seconds())
	ageScore := 1.0 / (1.0 + age.Seconds())
	
	return frequencyScore*0.5 + recencyScore*0.3 + ageScore*0.2
}