package cache

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// EvictionPolicy defines the cache eviction strategy
type EvictionPolicy int

const (
	PolicyLRU EvictionPolicy = iota
	PolicyLFU
	PolicyARC
	PolicyAdaptive
)

// AdaptiveCacheConfig configures the adaptive cache
type AdaptiveCacheConfig struct {
	Policy              EvictionPolicy
	MaxSize             int64
	LRUSize             int64  // For ARC: LRU partition size
	LFUSize             int64  // For ARC: LFU partition size
	AdaptationRate      float64 // How quickly to adapt between policies
	FrequencyThreshold  int     // Minimum access count for LFU
	RecencyWindow       time.Duration
	GhostCacheSize      int     // Size of ghost cache for ARC
}

// CacheEntry represents a cached item with metadata
type CacheEntry struct {
	Key           string
	Value         []byte
	Size          int64
	AccessCount   int
	LastAccess    time.Time
	CreatedAt     time.Time
	Frequency     float64
	Priority      float64
	Index         int // For heap operations
}

// AdaptiveCache implements multiple eviction policies with adaptive selection
type AdaptiveCache struct {
	config AdaptiveCacheConfig
	mu     sync.RWMutex
	
	// Storage
	entries map[string]*CacheEntry
	size    int64
	
	// LRU structures
	lruList  *lruList
	lruIndex map[string]*lruNode // FIX #11: separate index for LRU
	
	// LFU structures
	lfuHeap  *lfuHeap
	lfuIndex map[string]*CacheEntry // FIX #11: separate index for LFU
	
	// ARC structures (Adaptive Replacement Cache)
	t1      *lruList // Recent items (LRU)
	t2      *lruList // Frequent items (LFU)
	b1      *lruList // Ghost cache for T1
	b2      *lruList // Ghost cache for T2
	p       int64    // Target size for T1
	
	// Adaptive policy selection
	policyScores map[EvictionPolicy]float64
	recentHits   []policyHit
	
	// Metrics
	hits        int64
	misses      int64
	evictions   int64
	adaptations int64
}

type policyHit struct {
	policy    EvictionPolicy
	timestamp time.Time
	hit       bool
}

// NewAdaptiveCache creates a new adaptive cache
func NewAdaptiveCache(config AdaptiveCacheConfig) *AdaptiveCache {
	if config.AdaptationRate == 0 {
		config.AdaptationRate = 0.1
	}
	if config.FrequencyThreshold == 0 {
		config.FrequencyThreshold = 2
	}
	if config.RecencyWindow == 0 {
		config.RecencyWindow = 5 * time.Minute
	}
	if config.GhostCacheSize == 0 {
		config.GhostCacheSize = 1000
	}
	
	ac := &AdaptiveCache{
		config:       config,
		entries:      make(map[string]*CacheEntry),
		policyScores: make(map[EvictionPolicy]float64),
		recentHits:   make([]policyHit, 0, 1000),
		lruIndex:     make(map[string]*lruNode),
		lfuIndex:     make(map[string]*CacheEntry),
	}
	
	// Initialize policy scores
	ac.policyScores[PolicyLRU] = 1.0
	ac.policyScores[PolicyLFU] = 1.0
	ac.policyScores[PolicyARC] = 1.0
	
	// Initialize structures based on policy
	switch config.Policy {
	case PolicyLRU:
		ac.lruList = newLRUList()
	case PolicyLFU:
		ac.lfuHeap = newLFUHeap()
	case PolicyARC:
		ac.initARC()
	case PolicyAdaptive:
		// FIX #11: PolicyAdaptive uses ONLY lruList for eviction ordering.
		// The lfuHeap is NOT used in addToStructures/updateOnAccess for
		// PolicyAdaptive to avoid double-counting and heap corruption.
		// selectBestPolicy() will choose between LRU and LFU eviction
		// strategies at eviction time using the policyScores.
		ac.lruList = newLRUList()
		ac.initARC()
	}
	
	return ac
}

// Get retrieves an item from cache
func (ac *AdaptiveCache) Get(ctx context.Context, key string) ([]byte, bool) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	entry, exists := ac.entries[key]
	if !exists {
		ac.misses++
		return nil, false
	}
	
	// Update access metadata
	entry.AccessCount++
	entry.LastAccess = time.Now()
	entry.Frequency = ac.calculateFrequency(entry)
	
	// Update policy-specific structures
	ac.updateOnAccess(entry)
	
	ac.hits++
	
	// Record hit for adaptive learning
	if ac.config.Policy == PolicyAdaptive {
		ac.recordPolicyHit(ac.selectPolicy(entry), true)
	}
	
	return entry.Value, true
}

// Put adds an item to cache
func (ac *AdaptiveCache) Put(ctx context.Context, key string, value []byte) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	size := int64(len(value))
	
	// Check if entry exists
	if entry, exists := ac.entries[key]; exists {
		// Update existing entry
		ac.size -= entry.Size
		entry.Value = value
		entry.Size = size
		entry.AccessCount++
		entry.LastAccess = time.Now()
		entry.Frequency = ac.calculateFrequency(entry)
		ac.size += size
		
		ac.updateOnAccess(entry)
		return nil
	}
	
	// Evict if necessary
	for ac.size+size > ac.config.MaxSize {
		if err := ac.evict(); err != nil {
			return err
		}
	}
	
	// Create new entry
	entry := &CacheEntry{
		Key:         key,
		Value:       value,
		Size:        size,
		AccessCount: 1,
		LastAccess:  time.Now(),
		CreatedAt:   time.Now(),
		Frequency:   1.0,
	}
	
	ac.entries[key] = entry
	ac.size += size
	
	// Add to policy-specific structures
	ac.addToStructures(entry)
	
	return nil
}

// evict removes the least valuable item based on current policy
func (ac *AdaptiveCache) evict() error {
	var victim *CacheEntry
	
	switch ac.config.Policy {
	case PolicyLRU:
		victim = ac.evictLRU()
	case PolicyLFU:
		victim = ac.evictLFU()
	case PolicyARC:
		victim = ac.evictARC()
	case PolicyAdaptive:
		victim = ac.evictAdaptive()
	}
	
	if victim == nil {
		return nil
	}
	
	// FIX #11: clean up all index structures for the evicted entry
	ac.removeFromStructures(victim)
	delete(ac.entries, victim.Key)
	ac.size -= victim.Size
	ac.evictions++
	
	return nil
}

// removeFromStructures removes an entry from all tracking structures.
// FIX #11: centralised cleanup prevents stale index entries.
func (ac *AdaptiveCache) removeFromStructures(entry *CacheEntry) {
	switch ac.config.Policy {
	case PolicyLRU:
		if node, ok := ac.lruIndex[entry.Key]; ok {
			ac.lruList.removeNode(node)
			delete(ac.lruIndex, entry.Key)
		}
	case PolicyLFU:
		if _, ok := ac.lfuIndex[entry.Key]; ok {
			// entry.Index is maintained by the heap
			heap.Remove(ac.lfuHeap, entry.Index)
			delete(ac.lfuIndex, entry.Key)
		}
	case PolicyARC:
		// ARC eviction already removes from t1/t2 in evictARC
	case PolicyAdaptive:
		if node, ok := ac.lruIndex[entry.Key]; ok {
			ac.lruList.removeNode(node)
			delete(ac.lruIndex, entry.Key)
		}
	}
}

// evictLRU evicts using LRU policy
func (ac *AdaptiveCache) evictLRU() *CacheEntry {
	if ac.lruList.len == 0 {
		return nil
	}
	
	node := ac.lruList.back()
	if node == nil {
		return nil
	}
	
	entry := node.entry
	ac.lruList.remove(node)
	delete(ac.lruIndex, entry.Key) // FIX #11
	return entry
}

// evictLFU evicts using LFU policy
func (ac *AdaptiveCache) evictLFU() *CacheEntry {
	if ac.lfuHeap.Len() == 0 {
		return nil
	}
	
	entry := heap.Pop(ac.lfuHeap).(*CacheEntry)
	delete(ac.lfuIndex, entry.Key) // FIX #11
	return entry
}

// evictARC evicts using ARC policy
func (ac *AdaptiveCache) evictARC() *CacheEntry {
	// ARC eviction logic
	if ac.t1.len >= ac.p {
		// Evict from T1
		node := ac.t1.back()
		if node != nil {
			ac.t1.remove(node)
			// Move to ghost cache B1
			if ac.b1.len < int64(ac.config.GhostCacheSize) {
				ac.b1.pushFront(node.entry)
			}
			return node.entry
		}
	}
	
	// Evict from T2
	node := ac.t2.back()
	if node != nil {
		ac.t2.remove(node)
		// Move to ghost cache B2
		if ac.b2.len < int64(ac.config.GhostCacheSize) {
			ac.b2.pushFront(node.entry)
		}
		return node.entry
	}
	
	return nil
}

// evictAdaptive uses adaptive policy selection
func (ac *AdaptiveCache) evictAdaptive() *CacheEntry {
	// Select best policy based on recent performance
	bestPolicy := ac.selectBestPolicy()
	
	switch bestPolicy {
	case PolicyLRU:
		return ac.evictLRU()
	case PolicyLFU:
		// FIX #11: for adaptive mode we only maintain lruList, so fall back to LRU
		// when LFU is selected (LFU would require a separate heap maintained in sync).
		return ac.evictLRU()
	case PolicyARC:
		return ac.evictARC()
	default:
		return ac.evictLRU()
	}
}

// selectBestPolicy selects the best performing policy
func (ac *AdaptiveCache) selectBestPolicy() EvictionPolicy {
	maxScore := 0.0
	bestPolicy := PolicyLRU
	
	for policy, score := range ac.policyScores {
		if score > maxScore {
			maxScore = score
			bestPolicy = policy
		}
	}
	
	return bestPolicy
}

// selectPolicy selects policy for a specific entry
func (ac *AdaptiveCache) selectPolicy(entry *CacheEntry) EvictionPolicy {
	// Calculate scores for each policy
	lruScore := ac.calculateLRUScore(entry)
	lfuScore := ac.calculateLFUScore(entry)
	
	if lruScore > lfuScore {
		return PolicyLRU
	}
	return PolicyLFU
}

// calculateLRUScore calculates LRU score for an entry
func (ac *AdaptiveCache) calculateLRUScore(entry *CacheEntry) float64 {
	age := time.Since(entry.LastAccess)
	return 1.0 / (1.0 + age.Seconds())
}

// calculateLFUScore calculates LFU score for an entry
func (ac *AdaptiveCache) calculateLFUScore(entry *CacheEntry) float64 {
	return entry.Frequency
}

// calculateFrequency calculates access frequency with time decay
func (ac *AdaptiveCache) calculateFrequency(entry *CacheEntry) float64 {
	age := time.Since(entry.CreatedAt)
	decayFactor := 1.0 / (1.0 + age.Seconds()/ac.config.RecencyWindow.Seconds())
	return float64(entry.AccessCount) * decayFactor
}

// updateOnAccess updates structures when an entry is accessed
func (ac *AdaptiveCache) updateOnAccess(entry *CacheEntry) {
	switch ac.config.Policy {
	case PolicyLRU:
		ac.lruList.moveToFront(entry)
		// Keep lruIndex in sync
		if node := ac.lruList.find(entry); node != nil {
			ac.lruIndex[entry.Key] = node
		}
	case PolicyLFU:
		if _, ok := ac.lfuIndex[entry.Key]; ok {
			heap.Fix(ac.lfuHeap, entry.Index)
		}
	case PolicyARC:
		ac.updateARC(entry)
	case PolicyAdaptive:
		// FIX #11: only update lruList (no lfuHeap in adaptive mode)
		ac.lruList.moveToFront(entry)
		if node := ac.lruList.find(entry); node != nil {
			ac.lruIndex[entry.Key] = node
		}
	}
}

// addToStructures adds entry to policy-specific structures
func (ac *AdaptiveCache) addToStructures(entry *CacheEntry) {
	switch ac.config.Policy {
	case PolicyLRU:
		ac.lruList.pushFront(entry)
		if node := ac.lruList.find(entry); node != nil {
			ac.lruIndex[entry.Key] = node
		}
	case PolicyLFU:
		heap.Push(ac.lfuHeap, entry)
		ac.lfuIndex[entry.Key] = entry
	case PolicyARC:
		ac.t1.pushFront(entry)
	case PolicyAdaptive:
		// FIX #11: only add to lruList; do NOT also push to lfuHeap
		ac.lruList.pushFront(entry)
		if node := ac.lruList.find(entry); node != nil {
			ac.lruIndex[entry.Key] = node
		}
	}
}

// initARC initializes ARC structures
func (ac *AdaptiveCache) initARC() {
	ac.t1 = newLRUList()
	ac.t2 = newLRUList()
	ac.b1 = newLRUList()
	ac.b2 = newLRUList()
	ac.p = ac.config.MaxSize / 2
}

// updateARC updates ARC structures on access
func (ac *AdaptiveCache) updateARC(entry *CacheEntry) {
	// Check if in T1
	if ac.t1.contains(entry) {
		// Move to T2 (frequent)
		ac.t1.remove(ac.t1.find(entry))
		ac.t2.pushFront(entry)
		return
	}
	
	// Check if in T2
	if ac.t2.contains(entry) {
		// Move to front of T2
		ac.t2.moveToFront(entry)
		return
	}
	
	// Check ghost caches and adapt
	if ac.b1.contains(entry) {
		// Increase p (favor recency)
		delta := int64(1)
		if ac.b2.len > ac.b1.len {
			delta = ac.b2.len / ac.b1.len
		}
		ac.p = minInt64(ac.p+delta, ac.config.MaxSize) // FIX #12
		ac.adaptations++
	} else if ac.b2.contains(entry) {
		// Decrease p (favor frequency)
		delta := int64(1)
		if ac.b1.len > ac.b2.len {
			delta = ac.b1.len / ac.b2.len
		}
		ac.p = maxInt64(ac.p-delta, 0) // FIX #12
		ac.adaptations++
	}
}

// recordPolicyHit records a policy hit for adaptive learning
func (ac *AdaptiveCache) recordPolicyHit(policy EvictionPolicy, hit bool) {
	ac.recentHits = append(ac.recentHits, policyHit{
		policy:    policy,
		timestamp: time.Now(),
		hit:       hit,
	})
	
	// Keep only recent hits
	if len(ac.recentHits) > 1000 {
		ac.recentHits = ac.recentHits[1:]
	}
	
	// Update policy scores
	ac.updatePolicyScores()
}

// updatePolicyScores updates policy scores based on recent performance
func (ac *AdaptiveCache) updatePolicyScores() {
	// Calculate hit rates for each policy
	hitCounts := make(map[EvictionPolicy]int)
	totalCounts := make(map[EvictionPolicy]int)
	
	for _, hit := range ac.recentHits {
		totalCounts[hit.policy]++
		if hit.hit {
			hitCounts[hit.policy]++
		}
	}
	
	// Update scores with learning rate
	for policy := range ac.policyScores {
		if totalCounts[policy] > 0 {
			hitRate := float64(hitCounts[policy]) / float64(totalCounts[policy])
			ac.policyScores[policy] = ac.policyScores[policy]*(1-ac.config.AdaptationRate) +
				hitRate*ac.config.AdaptationRate
		}
	}
}

// GetStats returns cache statistics
func (ac *AdaptiveCache) GetStats() AdaptiveCacheStats {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	hitRate := 0.0
	if ac.hits+ac.misses > 0 {
		hitRate = float64(ac.hits) / float64(ac.hits+ac.misses)
	}
	
	return AdaptiveCacheStats{
		Size:        ac.size,
		Entries:     len(ac.entries),
		Hits:        ac.hits,
		Misses:      ac.misses,
		Evictions:   ac.evictions,
		HitRate:     hitRate,
		Adaptations: ac.adaptations,
		PolicyScores: ac.policyScores,
	}
}

// AdaptiveCacheStats contains cache statistics
type AdaptiveCacheStats struct {
	Size         int64
	Entries      int
	Hits         int64
	Misses       int64
	Evictions    int64
	HitRate      float64
	Adaptations  int64
	PolicyScores map[EvictionPolicy]float64
}

// FIX #12: renamed to avoid conflict with Go 1.21 built-in min/max.
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// lruList implements a doubly-linked list for LRU
type lruList struct {
	head *lruNode
	tail *lruNode
	len  int64
}

type lruNode struct {
	entry *CacheEntry
	prev  *lruNode
	next  *lruNode
}

func newLRUList() *lruList {
	return &lruList{}
}

func (l *lruList) pushFront(entry *CacheEntry) {
	node := &lruNode{entry: entry}
	
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	
	l.len++
}

func (l *lruList) remove(node *lruNode) {
	l.removeNode(node)
}

func (l *lruList) removeNode(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	
	node.prev = nil
	node.next = nil
	l.len--
}

func (l *lruList) back() *lruNode {
	return l.tail
}

func (l *lruList) moveToFront(entry *CacheEntry) {
	node := l.find(entry)
	if node != nil && node != l.head {
		l.removeNode(node)
		// Re-insert at front without allocating a new node
		node.prev = nil
		node.next = l.head
		if l.head != nil {
			l.head.prev = node
		}
		l.head = node
		if l.tail == nil {
			l.tail = node
		}
		l.len++
	}
}

func (l *lruList) find(entry *CacheEntry) *lruNode {
	for node := l.head; node != nil; node = node.next {
		if node.entry.Key == entry.Key {
			return node
		}
	}
	return nil
}

func (l *lruList) contains(entry *CacheEntry) bool {
	return l.find(entry) != nil
}

// lfuHeap implements a min-heap for LFU
type lfuHeap []*CacheEntry

func newLFUHeap() *lfuHeap {
	h := &lfuHeap{}
	heap.Init(h)
	return h
}

func (h lfuHeap) Len() int { return len(h) }

func (h lfuHeap) Less(i, j int) bool {
	return h[i].Frequency < h[j].Frequency
}

func (h lfuHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *lfuHeap) Push(x any) {
	entry := x.(*CacheEntry)
	entry.Index = len(*h)
	*h = append(*h, entry)
}

func (h *lfuHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.Index = -1
	*h = old[0 : n-1]
	return entry
}