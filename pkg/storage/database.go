package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Database provides a unified interface for persistent storage
type Database struct {
	db         *badger.DB
	config     DatabaseConfig
	mu         sync.RWMutex
	closed     bool
	shutdownCh chan struct{} // FIX #22: signals background goroutines to stop
	wg         sync.WaitGroup
}

// DatabaseConfig configures the database
type DatabaseConfig struct {
	Path              string
	MaxMemoryMB       int64
	SyncWrites        bool
	Compression       bool
	EncryptionKey     []byte
	GCInterval        time.Duration
	BackupPath        string
	BackupInterval    time.Duration
	BackupRetention   time.Duration
	ValueLogFileSize  int64
	NumVersionsToKeep int
}

// DefaultDatabaseConfig returns default configuration
func DefaultDatabaseConfig(path string) DatabaseConfig {
	return DatabaseConfig{
		Path:              path,
		MaxMemoryMB:       4096,
		SyncWrites:        false, // Async for performance
		Compression:       true,
		GCInterval:        10 * time.Minute,
		BackupInterval:    1 * time.Hour,
		BackupRetention:   7 * 24 * time.Hour,
		ValueLogFileSize:  1 << 30, // 1GB
		NumVersionsToKeep: 1,
	}
}

// TestDatabaseConfig returns a minimal configuration suitable for unit tests.
// Uses small memory limits to avoid OOM in test environments.
func TestDatabaseConfig(path string) DatabaseConfig {
	return DatabaseConfig{
		Path:              path,
		MaxMemoryMB:       32,
		SyncWrites:        false,
		Compression:       false,
		GCInterval:        5 * time.Minute,
		ValueLogFileSize:  1 << 20, // 1MB minimum
		NumVersionsToKeep: 1,
	}
}

// NewDatabase creates a new database instance
func NewDatabase(config DatabaseConfig) (*Database, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(config.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Configure BadgerDB options
	opts := badger.DefaultOptions(config.Path)
	opts.SyncWrites = config.SyncWrites
	if config.NumVersionsToKeep > 0 {
		opts.NumVersionsToKeep = config.NumVersionsToKeep
	}
	// BadgerDB requires ValueLogFileSize in [1MB, 2GB); apply default if not set
	if config.ValueLogFileSize >= 1<<20 && config.ValueLogFileSize <= 2<<30 {
		opts.ValueLogFileSize = config.ValueLogFileSize
	}

	// Enable compression
	// Compression disabled for compatibility

	// Enable encryption if key provided
	if config.EncryptionKey != nil {
		opts.EncryptionKey = config.EncryptionKey
		opts.EncryptionKeyRotationDuration = 10 * 24 * time.Hour
	}

	// Memory settings
	opts.MemTableSize = config.MaxMemoryMB * 1024 * 1024 / 4
	opts.ValueLogMaxEntries = uint32(1000000)

	// Open database
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	database := &Database{
		db:         db,
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	// Start background tasks (FIX #22: tracked by WaitGroup, stopped via shutdownCh)
	database.wg.Add(1)
	go database.runGarbageCollection()
	if config.BackupInterval > 0 && config.BackupPath != "" {
		database.wg.Add(1)
		go database.runBackups()
	}

	return database, nil
}

// Get retrieves a value by key
func (d *Database) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("database is closed")
	}

	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return value, err
}

// Set stores a key-value pair
func (d *Database) Set(key, value []byte) error {
	return d.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a key-value pair with TTL
func (d *Database) SetWithTTL(key, value []byte, ttl time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// Delete removes a key
func (d *Database) Delete(key []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Exists checks if a key exists
func (d *Database) Exists(key []byte) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return false, fmt.Errorf("database is closed")
	}

	err := d.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// SetBatch stores multiple key-value pairs atomically
func (d *Database) SetBatch(items map[string][]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	wb := d.db.NewWriteBatch()
	defer wb.Cancel()

	for key, value := range items {
		if err := wb.Set([]byte(key), value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// GetBatch retrieves multiple values
func (d *Database) GetBatch(keys [][]byte) (map[string][]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("database is closed")
	}

	results := make(map[string][]byte)

	err := d.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			results[string(key)] = value
		}
		return nil
	})

	return results, err
}

// Scan iterates over keys with a given prefix
func (d *Database) Scan(prefix []byte, fn func(key, value []byte) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err := fn(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}

// ScanKeys iterates over keys only (no values)
func (d *Database) ScanKeys(prefix []byte, fn func(key []byte) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			if err := fn(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Count counts keys with a given prefix
func (d *Database) Count(prefix []byte) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, fmt.Errorf("database is closed")
	}

	count := 0
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// DeletePrefix deletes all keys with a given prefix
func (d *Database) DeletePrefix(prefix []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Transaction executes a function within a transaction
func (d *Database) Transaction(fn func(txn *badger.Txn) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.Update(fn)
}

// ViewTransaction executes a read-only function within a transaction
func (d *Database) ViewTransaction(fn func(txn *badger.Txn) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	return d.db.View(fn)
}

// Backup creates a backup of the database
func (d *Database) Backup(path string) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	// Create backup directory
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	// Backup database
	_, err = d.db.Backup(f, 0)
	return err
}

// Restore restores the database from a backup
func (d *Database) Restore(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	// Open backup file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer f.Close()

	// Load backup
	return d.db.Load(f, 256)
}

// Stats returns database statistics
func (d *Database) Stats() DatabaseStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	lsm, vlog := d.db.Size()

	return DatabaseStats{
		LSMSize:   lsm,
		VLogSize:  vlog,
		TotalSize: lsm + vlog,
	}
}

// DatabaseStats contains database statistics
type DatabaseStats struct {
	LSMSize   int64
	VLogSize  int64
	TotalSize int64
}

// Close closes the database.
// FIX #22: signals background goroutines via shutdownCh and waits for them
// to exit before closing the underlying BadgerDB instance.
func (d *Database) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.shutdownCh)
	d.mu.Unlock()

	// Wait for all background goroutines to finish
	d.wg.Wait()

	return d.db.Close()
}

// runGarbageCollection runs periodic garbage collection.
// FIX #22: exits cleanly when shutdownCh is closed.
func (d *Database) runGarbageCollection() {
	defer d.wg.Done()

	gcInterval := d.config.GCInterval
	if gcInterval <= 0 {
		gcInterval = 5 * time.Minute // default GC interval
	}
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.shutdownCh:
			return
		case <-ticker.C:
			// Run GC
			err := d.db.RunValueLogGC(0.5)
			if err != nil && err != badger.ErrNoRewrite {
				// Log error (would use logger in production)
				continue
			}
		}
	}
}

// runBackups runs periodic backups.
// FIX #22: exits cleanly when shutdownCh is closed.
func (d *Database) runBackups() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.config.BackupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.shutdownCh:
			return
		case <-ticker.C:
			// Create backup
			timestamp := time.Now().Format("20060102-150405")
			backupPath := filepath.Join(d.config.BackupPath, fmt.Sprintf("backup-%s.db", timestamp))

			if err := d.Backup(backupPath); err != nil {
				// Log error (would use logger in production)
				continue
			}

			// Clean old backups
			d.cleanOldBackups()
		}
	}
}

// cleanOldBackups removes old backup files
func (d *Database) cleanOldBackups() {
	if d.config.BackupPath == "" {
		return
	}

	files, err := os.ReadDir(d.config.BackupPath)
	if err != nil {
		return
	}

	cutoff := time.Now().Add(-d.config.BackupRetention)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			os.Remove(filepath.Join(d.config.BackupPath, file.Name()))
		}
	}
}

// Namespace provides a namespaced view of the database
type Namespace struct {
	db     *Database
	prefix []byte
}

// NewNamespace creates a new namespace
func (d *Database) NewNamespace(prefix string) *Namespace {
	return &Namespace{
		db:     d,
		prefix: []byte(prefix + "/"),
	}
}

// Get retrieves a value from the namespace
func (n *Namespace) Get(key string) ([]byte, error) {
	return n.db.Get(n.key(key))
}

// Set stores a value in the namespace
func (n *Namespace) Set(key string, value []byte) error {
	return n.db.Set(n.key(key), value)
}

// Delete removes a key from the namespace
func (n *Namespace) Delete(key string) error {
	return n.db.Delete(n.key(key))
}

// Scan iterates over keys in the namespace
func (n *Namespace) Scan(fn func(key, value []byte) error) error {
	return n.db.Scan(n.prefix, func(key, value []byte) error {
		// Remove prefix from key
		relKey := key[len(n.prefix):]
		return fn(relKey, value)
	})
}

// Count counts keys in the namespace
func (n *Namespace) Count() (int, error) {
	return n.db.Count(n.prefix)
}

// Clear removes all keys in the namespace
func (n *Namespace) Clear() error {
	return n.db.DeletePrefix(n.prefix)
}

// key returns the full key with namespace prefix.
// Uses a fresh slice to avoid aliasing the prefix's backing array.
func (n *Namespace) key(key string) []byte {
	fullKey := make([]byte, len(n.prefix)+len(key))
	copy(fullKey, n.prefix)
	copy(fullKey[len(n.prefix):], key)
	return fullKey
}

// JSON helpers for structured data

// GetJSON retrieves and unmarshals JSON data
func (d *Database) GetJSON(key []byte, v any) error {
	data, err := d.Get(key)
	if err != nil {
		return err
	}
	if data == nil {
		return badger.ErrKeyNotFound
	}
	return json.Unmarshal(data, v)
}

// SetJSON marshals and stores JSON data
func (d *Database) SetJSON(key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return d.Set(key, data)
}

// GetJSON retrieves and unmarshals JSON data from namespace
func (n *Namespace) GetJSON(key string, v any) error {
	return n.db.GetJSON(n.key(key), v)
}

// SetJSON marshals and stores JSON data in namespace
func (n *Namespace) SetJSON(key string, v any) error {
	return n.db.SetJSON(n.key(key), v)
}