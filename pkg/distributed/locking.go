package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LockManager implements distributed locking
type LockManager struct {
	config LockConfig
	mu     sync.RWMutex

	// Lock tracking
	locks      map[string]*DistributedLock
	localLocks map[string]*LocalLock

	// Lease management
	leases map[string]*Lease

	// Deadlock detection
	waitGraph map[string][]string // node -> waiting for nodes

	// Statistics
	stats LockStats

	// Shutdown (FIX #26: wg tracks background goroutines for clean shutdown)
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// LockConfig configures the lock manager
type LockConfig struct {
	NodeID           string
	DefaultTimeout   time.Duration
	LeaseTimeout     time.Duration
	HeartbeatInterval time.Duration
	EnableDeadlockDetection bool
	MaxLockWaitTime  time.Duration
}

// DistributedLock represents a distributed lock
type DistributedLock struct {
	Key            string
	Owner          string
	AcquiredAt     time.Time
	ExpiresAt      time.Time
	Version        int64
	Waiters        []string  // nodes waiting to acquire (exclusive)
	SharedHolders  []string  // FIX #25: nodes currently holding a shared lock
	Type           LockType
	mu             sync.RWMutex
}

// LocalLock represents a local lock
type LocalLock struct {
	Key        string
	Owner      string
	AcquiredAt time.Time
	Count      int // For reentrant locks
}

// Lease represents a lock lease
type Lease struct {
	LockKey    string
	Owner      string
	ExpiresAt  time.Time
	Renewed    int
	mu         sync.RWMutex
}

// LockType defines the type of lock
type LockType int

const (
	LockTypeExclusive LockType = iota
	LockTypeShared
)

// LockStats contains locking statistics
type LockStats struct {
	LocksAcquired    int64
	LocksReleased    int64
	LockTimeouts     int64
	Deadlocks        int64
	AvgLockHoldTime  time.Duration
	AvgWaitTime      time.Duration
	ActiveLocks      int
}

// NewLockManager creates a new lock manager
func NewLockManager(config LockConfig) *LockManager {
	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = 30 * time.Second
	}
	if config.LeaseTimeout == 0 {
		config.LeaseTimeout = 10 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}
	if config.MaxLockWaitTime == 0 {
		config.MaxLockWaitTime = 60 * time.Second
	}
	
	lm := &LockManager{
		config:     config,
		locks:      make(map[string]*DistributedLock),
		localLocks: make(map[string]*LocalLock),
		leases:     make(map[string]*Lease),
		waitGraph:  make(map[string][]string),
		stopChan:   make(chan struct{}),
	}
	
	// Start background tasks (FIX #26: tracked by WaitGroup)
	lm.wg.Add(2)
	go lm.leaseRenewalLoop()
	go lm.lockCleanupLoop()
	if config.EnableDeadlockDetection {
		lm.wg.Add(1)
		go lm.deadlockDetectionLoop()
	}
	
	return lm
}

// Lock acquires a distributed lock
func (lm *LockManager) Lock(ctx context.Context, key string, lockType LockType) error {
	return lm.LockWithTimeout(ctx, key, lockType, lm.config.DefaultTimeout)
}

// LockWithTimeout acquires a lock with timeout
func (lm *LockManager) LockWithTimeout(ctx context.Context, key string, lockType LockType, timeout time.Duration) error {
	startTime := time.Now()
	
	// Try to acquire lock
	for {
		acquired, err := lm.tryAcquireLock(key, lockType)
		if err != nil {
			return err
		}
		
		if acquired {
			lm.stats.LocksAcquired++
			return nil
		}
		
		// Check timeout
		if time.Since(startTime) > timeout {
			lm.stats.LockTimeouts++
			return fmt.Errorf("lock timeout for key: %s", key)
		}
		
		// Add to wait graph for deadlock detection
		if lm.config.EnableDeadlockDetection {
			lm.addToWaitGraph(lm.config.NodeID, key)
		}
		
		// Wait and retry
		select {
		case <-time.After(100 * time.Millisecond):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryAcquireLock attempts to acquire a lock
func (lm *LockManager) tryAcquireLock(key string, lockType LockType) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lock, exists := lm.locks[key]
	
	if !exists {
		// Create new lock
		lock = &DistributedLock{
			Key:        key,
			Owner:      lm.config.NodeID,
			AcquiredAt: time.Now(),
			ExpiresAt:  time.Now().Add(lm.config.LeaseTimeout),
			Version:    1,
			Type:       lockType,
			Waiters:    make([]string, 0),
		}
		lm.locks[key] = lock
		
		// Create lease
		lm.createLease(key)
		
		lm.stats.ActiveLocks++
		return true, nil
	}
	
	// Check if lock is expired
	if time.Now().After(lock.ExpiresAt) {
		// Lock expired, acquire it
		lock.Owner = lm.config.NodeID
		lock.AcquiredAt = time.Now()
		lock.ExpiresAt = time.Now().Add(lm.config.LeaseTimeout)
		lock.Version++
		lock.Type = lockType
		
		lm.createLease(key)
		return true, nil
	}
	
	// Check if we already own the lock (reentrant)
	if lock.Owner == lm.config.NodeID {
		// Update expiration
		lock.ExpiresAt = time.Now().Add(lm.config.LeaseTimeout)
		return true, nil
	}
	
	// Check if shared lock and requesting shared
	// FIX #25: shared holders go into SharedHolders, not Waiters.
	// Waiters is reserved for nodes that are blocked waiting for the lock.
	if lock.Type == LockTypeShared && lockType == LockTypeShared {
		if !contains(lock.SharedHolders, lm.config.NodeID) {
			lock.SharedHolders = append(lock.SharedHolders, lm.config.NodeID)
		}
		return true, nil
	}
	
	// Lock is held by another node
	if !contains(lock.Waiters, lm.config.NodeID) {
		lock.Waiters = append(lock.Waiters, lm.config.NodeID)
	}
	
	return false, nil
}

// Unlock releases a distributed lock
func (lm *LockManager) Unlock(ctx context.Context, key string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lock, exists := lm.locks[key]
	if !exists {
		// Lock already released or never held — idempotent, return nil
		return nil
	}
	
	if lock.Owner != lm.config.NodeID {
		return fmt.Errorf("lock not owned by this node")
	}
	
	// Remove from wait graph
	if lm.config.EnableDeadlockDetection {
		delete(lm.waitGraph, lm.config.NodeID)
	}
	
	// Cancel lease
	delete(lm.leases, key)
	
	// Remove lock
	delete(lm.locks, key)
	
	lm.stats.LocksReleased++
	lm.stats.ActiveLocks--
	
	// Update hold time
	holdTime := time.Since(lock.AcquiredAt)
	lm.stats.AvgLockHoldTime = (lm.stats.AvgLockHoldTime + holdTime) / 2
	
	return nil
}

// TryLock attempts to acquire a lock without blocking
func (lm *LockManager) TryLock(ctx context.Context, key string, lockType LockType) (bool, error) {
	return lm.tryAcquireLock(key, lockType)
}

// createLease creates a lease for a lock
func (lm *LockManager) createLease(key string) {
	lease := &Lease{
		LockKey:   key,
		Owner:     lm.config.NodeID,
		ExpiresAt: time.Now().Add(lm.config.LeaseTimeout),
		Renewed:   0,
	}
	lm.leases[key] = lease
}

// leaseRenewalLoop periodically renews leases
func (lm *LockManager) leaseRenewalLoop() {
	defer lm.wg.Done() // FIX #26
	ticker := time.NewTicker(lm.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			lm.renewLeases()
			
		case <-lm.stopChan:
			return
		}
	}
}

// renewLeases renews all active leases
func (lm *LockManager) renewLeases() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	now := time.Now()
	
	for key, lease := range lm.leases {
		lease.mu.Lock()
		
		// Renew if close to expiration
		if now.Add(lm.config.HeartbeatInterval).After(lease.ExpiresAt) {
			lease.ExpiresAt = now.Add(lm.config.LeaseTimeout)
			lease.Renewed++
			
			// Update lock expiration
			if lock, exists := lm.locks[key]; exists {
				lock.ExpiresAt = lease.ExpiresAt
			}
		}
		
		lease.mu.Unlock()
	}
}

// lockCleanupLoop periodically cleans up expired locks
func (lm *LockManager) lockCleanupLoop() {
	defer lm.wg.Done() // FIX #26
	ticker := time.NewTicker(lm.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			lm.cleanupExpiredLocks()
			
		case <-lm.stopChan:
			return
		}
	}
}

// cleanupExpiredLocks removes expired locks
func (lm *LockManager) cleanupExpiredLocks() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	now := time.Now()
	
	for key, lock := range lm.locks {
		if now.After(lock.ExpiresAt) {
			delete(lm.locks, key)
			delete(lm.leases, key)
			lm.stats.ActiveLocks--
		}
	}
}

// deadlockDetectionLoop periodically checks for deadlocks
func (lm *LockManager) deadlockDetectionLoop() {
	defer lm.wg.Done() // FIX #26
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			lm.detectDeadlocks()
			
		case <-lm.stopChan:
			return
		}
	}
}

// detectDeadlocks detects and resolves deadlocks.
// FIX #27: previously held RLock while calling resolveDeadlock which deletes
// from lm.locks — a data race.  We now collect deadlocked nodes under RLock,
// release it, then acquire the write lock for resolution.
func (lm *LockManager) detectDeadlocks() {
	// Phase 1: detect cycles under read lock
	lm.mu.RLock()
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	deadlocked := make([]string, 0)
	for node := range lm.waitGraph {
		if lm.hasCycle(node, visited, recStack) {
			deadlocked = append(deadlocked, node)
		}
	}
	lm.mu.RUnlock()

	if len(deadlocked) == 0 {
		return
	}

	// Phase 2: resolve under write lock
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, node := range deadlocked {
		lm.stats.Deadlocks++
		lm.resolveDeadlock(node)
	}
}

// hasCycle detects cycles in wait graph using DFS
func (lm *LockManager) hasCycle(node string, visited, recStack map[string]bool) bool {
	visited[node] = true
	recStack[node] = true
	
	for _, neighbor := range lm.waitGraph[node] {
		if !visited[neighbor] {
			if lm.hasCycle(neighbor, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
			return true
		}
	}
	
	recStack[node] = false
	return false
}

// resolveDeadlock resolves a detected deadlock
func (lm *LockManager) resolveDeadlock(node string) {
	// Simple resolution: abort the youngest transaction
	// In production, use more sophisticated algorithms
	
	// Find lock with shortest hold time
	var victimKey string
	minHoldTime := time.Duration(1<<63 - 1)
	
	for key, lock := range lm.locks {
		holdTime := time.Since(lock.AcquiredAt)
		if holdTime < minHoldTime {
			minHoldTime = holdTime
			victimKey = key
		}
	}
	
	if victimKey != "" {
		// Force release the lock
		delete(lm.locks, victimKey)
		delete(lm.leases, victimKey)
	}
}

// addToWaitGraph adds an edge to the wait graph
func (lm *LockManager) addToWaitGraph(waiter, lockKey string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lock, exists := lm.locks[lockKey]
	if !exists {
		return
	}
	
	if !contains(lm.waitGraph[waiter], lock.Owner) {
		lm.waitGraph[waiter] = append(lm.waitGraph[waiter], lock.Owner)
	}
}

// GetLockInfo returns information about a lock
func (lm *LockManager) GetLockInfo(key string) (*DistributedLock, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	lock, exists := lm.locks[key]
	if !exists {
		return nil, fmt.Errorf("lock not found: %s", key)
	}
	
	// Return a copy of the lock data (without copying the mutex)
	lock.mu.RLock()
	lockCopy := &DistributedLock{
		Key:        lock.Key,
		Owner:      lock.Owner,
		AcquiredAt: lock.AcquiredAt,
		ExpiresAt:  lock.ExpiresAt,
		Version:    lock.Version,
		Type:       lock.Type,
	}
	if lock.Waiters != nil {
		lockCopy.Waiters = make([]string, len(lock.Waiters))
		copy(lockCopy.Waiters, lock.Waiters)
	}
	lock.mu.RUnlock()
	return lockCopy, nil
}

// ExportLocks returns a snapshot of all current locks for persistence.
// The returned map is a deep copy; callers may safely marshal it to JSON.
func (lm *LockManager) ExportLocks() map[string]*DistributedLock {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	snap := make(map[string]*DistributedLock, len(lm.locks))
	for k, v := range lm.locks {
		copy := *v
		if v.Waiters != nil {
			copy.Waiters = append([]string(nil), v.Waiters...)
		}
		if v.SharedHolders != nil {
			copy.SharedHolders = append([]string(nil), v.SharedHolders...)
		}
		snap[k] = &copy
	}
	return snap
}

// ImportLocks restores locks from a persisted snapshot.
// Existing in-memory locks are replaced.
func (lm *LockManager) ImportLocks(locks map[string]*DistributedLock) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for k, v := range locks {
		// Only restore locks that have not yet expired
		if time.Now().Before(v.ExpiresAt) {
			lm.locks[k] = v
		}
	}
	lm.stats.ActiveLocks = len(lm.locks)
}

// ExportLeases returns a snapshot of all current leases for persistence.
func (lm *LockManager) ExportLeases() map[string]*Lease {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	snap := make(map[string]*Lease, len(lm.leases))
	for k, v := range lm.leases {
		copy := *v
		snap[k] = &copy
	}
	return snap
}

// ImportLeases restores leases from a persisted snapshot.
func (lm *LockManager) ImportLeases(leases map[string]*Lease) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for k, v := range leases {
		if time.Now().Before(v.ExpiresAt) {
			lm.leases[k] = v
		}
	}
}

// GetStats returns locking statistics
func (lm *LockManager) GetStats() LockStats {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	return lm.stats
}

// Stop stops the lock manager
// Stop stops all background goroutines and waits for them to finish.
// FIX #26: now drains the WaitGroup so goroutines are fully stopped.
func (lm *LockManager) Stop() {
	select {
	case <-lm.stopChan:
		return // already closed
	default:
		close(lm.stopChan)
	}
	lm.wg.Wait()
}

// Helper function
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}