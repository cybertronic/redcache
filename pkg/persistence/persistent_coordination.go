package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"redcache/pkg/distributed"
	"redcache/pkg/storage"
)

// persistedLock is the JSON-serialisable form of a distributed lock.
// sync.RWMutex cannot be marshalled, so we project only the data fields.
type persistedLock struct {
	Key           string    `json:"key"`
	Owner         string    `json:"owner"`
	AcquiredAt    time.Time `json:"acquired_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Version       int64     `json:"version"`
	Waiters       []string  `json:"waiters,omitempty"`
	SharedHolders []string  `json:"shared_holders,omitempty"`
	Type          int       `json:"type"`
}

// persistedLease is the JSON-serialisable form of a lease.
type persistedLease struct {
	LockKey   string    `json:"lock_key"`
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
	Renewed   int       `json:"renewed"`
}

// PersistentLockManager extends LockManager with database persistence.
// FIX #10: flush() and load() now perform real serialisation/deserialisation
// via the nsLocks and nsLeases namespaces instead of being no-op stubs.
type PersistentLockManager struct {
	*distributed.LockManager
	db       *storage.Database
	nsLocks  *storage.Namespace
	nsLeases *storage.Namespace
	mu       sync.RWMutex
	flushCh  chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewPersistentLockManager creates a new persistent lock manager
func NewPersistentLockManager(config distributed.LockConfig, db *storage.Database) (*PersistentLockManager, error) {
	baseLockMgr := distributed.NewLockManager(config)

	nsLocks := db.NewNamespace("locks")
	nsLeases := db.NewNamespace("leases")

	plm := &PersistentLockManager{
		LockManager: baseLockMgr,
		db:          db,
		nsLocks:     nsLocks,
		nsLeases:    nsLeases,
		flushCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}

	// Restore persisted state before accepting new operations
	if err := plm.load(); err != nil {
		return nil, fmt.Errorf("failed to load persisted locks: %w", err)
	}

	// Start flush worker
	plm.wg.Add(1)
	go plm.flushWorker()

	return plm, nil
}

// Lock acquires a lock and persists it
func (plm *PersistentLockManager) Lock(ctx context.Context, key string, lockType distributed.LockType) error {
	err := plm.LockManager.Lock(ctx, key, lockType)
	if err == nil {
		plm.triggerFlush()
	}
	return err
}

// Unlock releases a lock and persists the change
func (plm *PersistentLockManager) Unlock(ctx context.Context, key string) error {
	err := plm.LockManager.Unlock(ctx, key)
	if err == nil {
		plm.triggerFlush()
	}
	return err
}

// triggerFlush triggers a flush operation (non-blocking)
func (plm *PersistentLockManager) triggerFlush() {
	select {
	case plm.flushCh <- struct{}{}:
	default:
	}
}

// flushWorker periodically flushes locks to database
func (plm *PersistentLockManager) flushWorker() {
	defer plm.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			plm.flush()
		case <-plm.flushCh:
			plm.flush()
		case <-plm.stopCh:
			plm.flush() // Final flush before exit
			return
		}
	}
}

// flush persists all current locks and leases to the database.
// FIX #10: previously a no-op stub.  Now serialises each lock/lease to JSON
// and writes it to the appropriate namespace using the lock key as the DB key.
func (plm *PersistentLockManager) flush() {
	// Snapshot current state via the exported accessors on LockManager.
	locks := plm.LockManager.ExportLocks()
	leases := plm.LockManager.ExportLeases()

	// Persist locks
	for key, lock := range locks {
		pl := persistedLock{
			Key:           lock.Key,
			Owner:         lock.Owner,
			AcquiredAt:    lock.AcquiredAt,
			ExpiresAt:     lock.ExpiresAt,
			Version:       lock.Version,
			Waiters:       lock.Waiters,
			SharedHolders: lock.SharedHolders,
			Type:          int(lock.Type),
		}
		data, err := json.Marshal(pl)
		if err != nil {
			continue
		}
		_ = plm.nsLocks.Set(key, data)
	}

	// Persist leases
	for key, lease := range leases {
		pl := persistedLease{
			LockKey:   lease.LockKey,
			Owner:     lease.Owner,
			ExpiresAt: lease.ExpiresAt,
			Renewed:   lease.Renewed,
		}
		data, err := json.Marshal(pl)
		if err != nil {
			continue
		}
		_ = plm.nsLeases.Set(key, data)
	}
}

// load restores locks and leases from the database.
// FIX #10: previously a no-op stub.  Now reads every key from the locks and
// leases namespaces, deserialises the JSON, and imports them into the embedded
// LockManager via the ExportLocks/ImportLocks accessors.
func (plm *PersistentLockManager) load() error {
	// Load locks
	restoredLocks := make(map[string]*distributed.DistributedLock)
	err := plm.nsLocks.Scan(func(key, value []byte) error {
		var pl persistedLock
		if err := json.Unmarshal(value, &pl); err != nil {
			return nil // skip corrupt entries
		}
		restoredLocks[pl.Key] = &distributed.DistributedLock{
			Key:           pl.Key,
			Owner:         pl.Owner,
			AcquiredAt:    pl.AcquiredAt,
			ExpiresAt:     pl.ExpiresAt,
			Version:       pl.Version,
			Waiters:       pl.Waiters,
			SharedHolders: pl.SharedHolders,
			Type:          distributed.LockType(pl.Type),
		}
		return nil
	})
	if err != nil {
		// An empty namespace returns nil from Scan; only propagate real errors.
		return fmt.Errorf("scanning locks namespace: %w", err)
	}
	if len(restoredLocks) > 0 {
		plm.LockManager.ImportLocks(restoredLocks)
	}

	// Load leases
	restoredLeases := make(map[string]*distributed.Lease)
	err = plm.nsLeases.Scan(func(key, value []byte) error {
		var pl persistedLease
		if err := json.Unmarshal(value, &pl); err != nil {
			return nil // skip corrupt entries
		}
		restoredLeases[pl.LockKey] = &distributed.Lease{
			LockKey:   pl.LockKey,
			Owner:     pl.Owner,
			ExpiresAt: pl.ExpiresAt,
			Renewed:   pl.Renewed,
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scanning leases namespace: %w", err)
	}
	if len(restoredLeases) > 0 {
		plm.LockManager.ImportLeases(restoredLeases)
	}

	return nil
}

// GetStats returns lock manager statistics
func (plm *PersistentLockManager) GetStats() map[string]any {
	baseStats := plm.LockManager.GetStats()

	locksInDB, _ := plm.nsLocks.Count()
	leasesInDB, _ := plm.nsLeases.Count()

	stats := make(map[string]any)
	stats["base"] = baseStats
	stats["locks_in_memory"] = len(plm.LockManager.ExportLocks())
	stats["locks_in_db"] = locksInDB
	stats["leases_in_db"] = leasesInDB

	return stats
}

// Close closes the persistent lock manager
func (plm *PersistentLockManager) Close() error {
	select {
	case <-plm.stopCh:
		return nil // already closed
	default:
		close(plm.stopCh)
	}
	plm.wg.Wait()
	return nil
}