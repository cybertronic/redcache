//go:build linux

package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"redcache/pkg/iouring"
)

// Storage implements disk-backed cache with LRU eviction.
// When an iouring.FilePool is attached via WithFilePool(), file reads and
// writes use io_uring fixed-buffer I/O (zero kernel↔user copy for reads,
// one copy for writes). Falls back to standard os.ReadFile/WriteFile
// transparently when io_uring is unavailable.
type Storage struct {
	mu                sync.RWMutex
	directory         string
	data              map[string]*CacheItem
	lru               *LRUList
	maxSize           int64
	currSize          int64
	hits              int64
	misses            int64
	evictions         int64
	evictionPolicy    string
	evictionThreshold float64

	// io_uring file pool (nil = use standard I/O)
	filePool *iouring.FilePool
}

// CacheItem represents a cached item
type CacheItem struct {
	Key        string
	Value      []byte
	Size       int64
	AccessTime time.Time
	CreateTime time.Time
	FilePath   string
}

// cacheItemMeta is the on-disk sidecar that maps a hash filename back to the
// original cache key.  Stored as <hash>.meta alongside <hash>.
type cacheItemMeta struct {
	Key string `json:"key"`
}

// StorageStats contains cache statistics
type StorageStats struct {
	Size           int64
	MaxSize        int64
	Entries        int
	Hits           int64
	Misses         int64
	Evictions      int64
	HitRate        float64
	IOUringEnabled bool
}

// LRUList implements a doubly-linked LRU list with O(1) lookup via an index map.
type LRUList struct {
	head  *LRUNode
	tail  *LRUNode
	size  int
	index map[string]*LRUNode // FIX #1: O(1) lookup instead of O(n) scan
}

// LRUNode represents a node in the LRU list
type LRUNode struct {
	key  string
	prev *LRUNode
	next *LRUNode
}

// StorageOption is a functional option for Storage
type StorageOption func(*Storage)

// WithEvictionPolicy sets the eviction policy
func WithEvictionPolicy(policy string) StorageOption {
	return func(s *Storage) {
		s.evictionPolicy = policy
	}
}

// WithEvictionThreshold sets the eviction threshold
func WithEvictionThreshold(threshold float64) StorageOption {
	return func(s *Storage) {
		s.evictionThreshold = threshold
	}
}

// WithFilePool attaches an io_uring FilePool for zero-copy file I/O.
// When set, Get() uses fixed-buffer reads and Put() uses fixed-buffer writes.
// If pool is nil or disabled, standard I/O is used.
func WithFilePool(pool *iouring.FilePool) StorageOption {
	return func(s *Storage) {
		s.filePool = pool
	}
}

// NewStorage creates a new cache storage
func NewStorage(directory string, maxSize int64, opts ...StorageOption) (*Storage, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	s := &Storage{
		directory:         directory,
		data:              make(map[string]*CacheItem),
		lru:               NewLRUList(),
		maxSize:           maxSize,
		evictionPolicy:    "lru",
		evictionThreshold: 0.9,
	}

	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	// Load existing cache entries
	if err := s.loadExistingEntries(); err != nil {
		return nil, fmt.Errorf("failed to load existing entries: %w", err)
	}

	return s, nil
}

// keyToFilename returns the hash-based filename for a cache key.
func keyToFilename(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// metaPath returns the path of the sidecar metadata file for a given hash filename.
func metaPath(directory, filename string) string {
	return filepath.Join(directory, filename+".meta")
}

// loadExistingEntries loads cache entries from disk.
// FIX #3: reads the .meta sidecar to recover the original key so that the
// in-memory map is keyed by the original key, not the SHA-256 filename.
func (s *Storage) loadExistingEntries() error {
	files, err := os.ReadDir(s.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		// Skip sidecar files – they are processed together with the data file.
		if len(name) > 5 && name[len(name)-5:] == ".meta" {
			continue
		}

		filePath := filepath.Join(s.directory, name)
		info, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// Read the sidecar to get the original key.
		meta := cacheItemMeta{}
		metaData, err := os.ReadFile(metaPath(s.directory, name))
		if err != nil {
			// No sidecar – orphaned data file; skip it.
			continue
		}
		if err := json.Unmarshal(metaData, &meta); err != nil || meta.Key == "" {
			continue
		}

		item := &CacheItem{
			Key:        meta.Key,
			Size:       info.Size(),
			CreateTime: info.ModTime(),
			AccessTime: info.ModTime(),
			FilePath:   filePath,
		}

		s.data[meta.Key] = item
		s.lru.AddToFront(meta.Key)
		s.currSize += info.Size()
	}

	return nil
}

// readFile reads a file using io_uring fixed-buffer I/O when available,
// falling back to os.ReadFile otherwise.
func (s *Storage) readFile(ctx context.Context, path string) ([]byte, error) {
	if s.filePool != nil && s.filePool.Enabled() {
		data, err := s.filePool.ReadFile(ctx, path)
		if err == nil {
			return data, nil
		}
		// Fall through to standard I/O on error
	}
	return os.ReadFile(path)
}

// writeFile writes a file using io_uring fixed-buffer I/O when available,
// falling back to os.WriteFile otherwise.
func (s *Storage) writeFile(ctx context.Context, path string, data []byte) error {
	if s.filePool != nil && s.filePool.Enabled() {
		err := s.filePool.WriteFile(ctx, path, data, 0644)
		if err == nil {
			return nil
		}
		// Fall through to standard I/O on error
	}
	return os.WriteFile(path, data, 0644)
}

// removeFile removes a file and its sidecar, logging errors instead of
// silently ignoring them (FIX #40).
func (s *Storage) removeFile(filePath string) {
	if filePath == "" {
		return
	}
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		// In production this would go to a structured logger.
		fmt.Fprintf(os.Stderr, "cache: failed to remove file %s: %v\n", filePath, err)
	}
	// Also remove the sidecar.
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)
	mp := metaPath(dir, base)
	if err := os.Remove(mp); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "cache: failed to remove meta file %s: %v\n", mp, err)
	}
}

// Get retrieves an item from cache.
// FIX #2: uses RLock for the lookup; only upgrades to a write lock when
// in-memory state (LRU position, access time, hit counter) must be mutated.
// File reads happen outside any lock.
func (s *Storage) Get(ctx context.Context, key string) ([]byte, bool) {
	// --- read-locked lookup ---
	s.mu.RLock()
	item, exists := s.data[key]
	s.mu.RUnlock()

	if !exists {
		s.mu.Lock()
		s.misses++
		s.mu.Unlock()
		return nil, false
	}

	// If the value is already in memory, return it under a read lock.
	s.mu.RLock()
	if item.Value != nil {
		value := item.Value
		s.mu.RUnlock()

		// Update LRU / stats under write lock (no I/O here).
		s.mu.Lock()
		item.AccessTime = time.Now()
		s.lru.MoveToFront(key)
		s.hits++
		s.mu.Unlock()
		return value, true
	}
	filePath := item.FilePath
	s.mu.RUnlock()

	// Read from disk WITHOUT holding any lock.
	data, err := s.readFile(ctx, filePath)
	if err != nil {
		s.mu.Lock()
		s.misses++
		s.mu.Unlock()
		return nil, false
	}

	// Write lock only to update in-memory state.
	s.mu.Lock()
	item.Value = data
	item.AccessTime = time.Now()
	s.lru.MoveToFront(key)
	s.hits++
	s.mu.Unlock()

	return data, true
}

// GetAllKeys returns all keys in the storage
func (s *Storage) GetAllKeys(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys, nil
}

// Put stores an item in cache.
// File writes use io_uring fixed-buffer I/O when a FilePool is attached.
func (s *Storage) Put(ctx context.Context, key string, value []byte) error {
	size := int64(len(value))

	// Generate filename from key hash
	filename := keyToFilename(key)
	filePath := filepath.Join(s.directory, filename)

	// Write to disk WITHOUT holding any lock.
	if err := s.writeFile(ctx, filePath, value); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	// Write sidecar metadata so we can recover the original key on restart.
	metaBytes, err := json.Marshal(cacheItemMeta{Key: key})
	if err != nil {
		return fmt.Errorf("failed to marshal cache meta: %w", err)
	}
	if err := os.WriteFile(metaPath(s.directory, filename), metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to write cache meta: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if item already exists
	if existing, exists := s.data[key]; exists {
		s.currSize -= existing.Size
		delete(s.data, key)
		s.lru.Remove(key)
		// Remove old file (different hash if key was re-used with different content)
		if existing.FilePath != "" && existing.FilePath != filePath {
			s.removeFile(existing.FilePath)
		}
	}

	// Evict items if necessary
	for s.currSize+size > s.maxSize && s.lru.size > 0 {
		evictKey := s.lru.RemoveLast()
		if evictItem, exists := s.data[evictKey]; exists {
			s.currSize -= evictItem.Size
			delete(s.data, evictKey)
			s.removeFile(evictItem.FilePath)
			s.evictions++
		}
	}

	// Add new item
	item := &CacheItem{
		Key:        key,
		Value:      value,
		Size:       size,
		AccessTime: time.Now(),
		CreateTime: time.Now(),
		FilePath:   filePath,
	}

	s.data[key] = item
	s.lru.AddToFront(key)
	s.currSize += size

	return nil
}

// Delete removes an item from cache
func (s *Storage) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, exists := s.data[key]; exists {
		s.currSize -= item.Size
		delete(s.data, key)
		s.lru.Remove(key)
		s.removeFile(item.FilePath)
	}

	return nil
}

// Stats returns cache statistics
func (s *Storage) Stats() StorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hitRate := 0.0
	if s.hits+s.misses > 0 {
		hitRate = float64(s.hits) / float64(s.hits+s.misses)
	}

	iouringEnabled := s.filePool != nil && s.filePool.Enabled()

	return StorageStats{
		Size:           s.currSize,
		MaxSize:        s.maxSize,
		Entries:        len(s.data),
		Hits:           s.hits,
		Misses:         s.misses,
		Evictions:      s.evictions,
		HitRate:        hitRate,
		IOUringEnabled: iouringEnabled,
	}
}

// Evict removes items to free up specified amount of space
func (s *Storage) Evict(targetSize int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	freedSize := int64(0)

	for freedSize < targetSize && s.lru.size > 0 {
		evictKey := s.lru.RemoveLast()
		if evictItem, exists := s.data[evictKey]; exists {
			s.currSize -= evictItem.Size
			freedSize += evictItem.Size
			delete(s.data, evictKey)
			s.removeFile(evictItem.FilePath)
			s.evictions++
		}
	}

	return nil
}

// Cleanup removes orphaned files and updates metadata
func (s *Storage) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get all files in cache directory
	files, err := os.ReadDir(s.directory)
	if err != nil {
		return err
	}

	// Check each file
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		// Skip sidecar files
		if len(name) > 5 && name[len(name)-5:] == ".meta" {
			continue
		}

		filePath := filepath.Join(s.directory, name)

		// Check if file is tracked
		found := false
		for _, item := range s.data {
			if item.FilePath == filePath {
				found = true
				break
			}
		}

		// Remove orphaned files (and their sidecars)
		if !found {
			s.removeFile(filePath)
		}
	}

	return nil
}

// Close closes the storage and releases resources
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear in-memory data
	s.data = make(map[string]*CacheItem)
	s.lru = NewLRUList()
	s.currSize = 0

	return nil
}

// LRU List implementation

// NewLRUList creates a new LRU list
func NewLRUList() *LRUList {
	return &LRUList{
		index: make(map[string]*LRUNode), // FIX #1
	}
}

// AddToFront adds a key to the front of the list
func (l *LRUList) AddToFront(key string) {
	// FIX #1: if already present, just move to front
	if _, exists := l.index[key]; exists {
		l.MoveToFront(key)
		return
	}

	node := &LRUNode{key: key}
	l.index[key] = node // FIX #1

	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}

	l.size++
}

// MoveToFront moves a key to the front of the list
func (l *LRUList) MoveToFront(key string) {
	// FIX #1: O(1) lookup via index map
	node := l.index[key]
	if node == nil || node == l.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == l.tail {
		l.tail = node.prev
	}

	// Add to front
	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}

// Remove removes a key from the list
func (l *LRUList) Remove(key string) {
	// FIX #1: O(1) lookup via index map
	node := l.index[key]
	if node == nil {
		return
	}

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

	delete(l.index, key) // FIX #1
	l.size--
}

// RemoveLast removes and returns the last key
func (l *LRUList) RemoveLast() string {
	if l.tail == nil {
		return ""
	}

	key := l.tail.key
	l.Remove(key)
	return key
}

// Prefetcher implements cache prefetching
type Prefetcher struct {
	storage  *Storage
	s3Client any
	config   any
}

// NewPrefetcher creates a new prefetcher
func NewPrefetcher(storage *Storage, s3Client any, config any) *Prefetcher {
	return &Prefetcher{
		storage:  storage,
		s3Client: s3Client,
		config:   config,
	}
}

// Run starts the prefetcher
func (p *Prefetcher) Run(ctx context.Context) {
	// Prefetching logic would go here
	<-ctx.Done()
}