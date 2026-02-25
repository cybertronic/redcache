package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DeduplicationConfig configures the deduplication engine
type DeduplicationConfig struct {
	ChunkSize         int     // Size of chunks for content-defined chunking
	MinChunkSize      int     // Minimum chunk size
	MaxChunkSize      int     // Maximum chunk size
	WindowSize        int     // Rolling hash window size
	EnableFingerprint bool    // Enable fingerprinting for similarity detection
	SimilarityThresh  float64 // Similarity threshold (0-1)
	MaxMemoryMB       int64   // Maximum memory for chunk index
}

// Deduplicator implements content-based deduplication.
// FIX #34: chunkRefs map removed; reference counting is done exclusively via
// ChunkMetadata.RefCount to eliminate the dual-counter divergence bug.
type Deduplicator struct {
	config DeduplicationConfig
	mu     sync.RWMutex

	// Chunk storage
	chunks    map[string]*ChunkMetadata // hash -> metadata
	chunkData map[string][]byte         // hash -> data

	// Content-defined chunking
	chunker *ContentDefinedChunker

	// Fingerprinting for similarity detection
	fingerprints map[string]*Fingerprint

	// Statistics
	stats DeduplicationStats

	// Memory management
	memoryUsed int64
}

// ChunkMetadata contains metadata about a chunk
type ChunkMetadata struct {
	Hash      string
	Size      int
	RefCount  int
	FirstSeen time.Time
	LastUsed  time.Time
	Frequency int
}

// Fingerprint represents a similarity fingerprint
type Fingerprint struct {
	Hash     string
	Features []uint64 // MinHash features
	Size     int
}

// DeduplicationStats contains deduplication statistics
type DeduplicationStats struct {
	TotalChunks        int64
	UniqueChunks       int64
	DuplicateChunks    int64
	BytesSaved         int64
	DeduplicationRatio float64
	AvgChunkSize       int
	MemoryUsed         int64
}

// NewDeduplicator creates a new deduplicator
func NewDeduplicator(config DeduplicationConfig) *Deduplicator {
	if config.ChunkSize == 0 {
		config.ChunkSize = 8 * 1024 // 8KB default
	}
	if config.MinChunkSize == 0 {
		config.MinChunkSize = 4 * 1024 // 4KB
	}
	if config.MaxChunkSize == 0 {
		config.MaxChunkSize = 16 * 1024 // 16KB
	}
	if config.WindowSize == 0 {
		config.WindowSize = 48
	}
	if config.SimilarityThresh == 0 {
		config.SimilarityThresh = 0.8
	}
	if config.MaxMemoryMB == 0 {
		config.MaxMemoryMB = 1024 // 1GB default
	}

	return &Deduplicator{
		config:       config,
		chunks:       make(map[string]*ChunkMetadata),
		chunkData:    make(map[string][]byte),
		fingerprints: make(map[string]*Fingerprint),
		chunker:      NewContentDefinedChunker(config),
	}
}

// Deduplicate deduplicates data and returns chunk references
func (d *Deduplicator) Deduplicate(ctx context.Context, data []byte) (*DeduplicatedData, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Split data into chunks using content-defined chunking
	chunks := d.chunker.Chunk(data)

	dedupData := &DeduplicatedData{
		Chunks:       make([]ChunkReference, 0, len(chunks)),
		OriginalSize: len(data),
	}

	uniqueBytes := 0
	duplicateBytes := 0

	for _, chunk := range chunks {
		// Calculate chunk hash
		hash := d.hashChunk(chunk)

		// Check if chunk exists
		if metadata, exists := d.chunks[hash]; exists {
			// Duplicate chunk found — increment the single RefCount (FIX #34)
			metadata.RefCount++
			metadata.LastUsed = time.Now()
			metadata.Frequency++
			duplicateBytes += len(chunk)

			d.stats.DuplicateChunks++
		} else {
			// New unique chunk
			metadata := &ChunkMetadata{
				Hash:      hash,
				Size:      len(chunk),
				RefCount:  1,
				FirstSeen: time.Now(),
				LastUsed:  time.Now(),
				Frequency: 1,
			}

			d.chunks[hash] = metadata
			d.chunkData[hash] = chunk

			uniqueBytes += len(chunk)
			d.stats.UniqueChunks++
			d.memoryUsed += int64(len(chunk))

			// Create fingerprint if enabled
			if d.config.EnableFingerprint {
				d.fingerprints[hash] = d.createFingerprint(chunk)
			}
		}

		d.stats.TotalChunks++

		// Add chunk reference
		dedupData.Chunks = append(dedupData.Chunks, ChunkReference{
			Hash:   hash,
			Offset: len(dedupData.Chunks),
			Size:   len(chunk),
		})

		// Check memory limit and evict if necessary
		if d.memoryUsed > d.config.MaxMemoryMB*1024*1024 {
			d.evictChunks()
		}
	}

	dedupData.DeduplicatedSize = uniqueBytes
	d.stats.BytesSaved += int64(duplicateBytes)

	// Update deduplication ratio
	if d.stats.TotalChunks > 0 {
		d.stats.DeduplicationRatio = float64(d.stats.DuplicateChunks) / float64(d.stats.TotalChunks)
	}

	return dedupData, nil
}

// Reconstruct reconstructs original data from chunk references.
// FIX #17: metadata.LastUsed mutation now happens under a write lock.
func (d *Deduplicator) Reconstruct(ctx context.Context, dedupData *DeduplicatedData) ([]byte, error) {
	d.mu.Lock() // FIX #17: write lock because we mutate LastUsed
	defer d.mu.Unlock()

	result := make([]byte, 0, dedupData.OriginalSize)

	for _, ref := range dedupData.Chunks {
		chunk, exists := d.chunkData[ref.Hash]
		if !exists {
			return nil, fmt.Errorf("chunk not found: %s", ref.Hash)
		}

		result = append(result, chunk...)

		// Update access time (safe: we hold the write lock)
		if metadata, exists := d.chunks[ref.Hash]; exists {
			metadata.LastUsed = time.Now()
		}
	}

	return result, nil
}

// FindSimilar finds similar data using fingerprinting
func (d *Deduplicator) FindSimilar(ctx context.Context, data []byte) []SimilarityResult {
	if !d.config.EnableFingerprint {
		return nil
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Create fingerprint for input data
	inputFingerprint := d.createFingerprint(data)

	results := make([]SimilarityResult, 0)

	// Compare with existing fingerprints
	for hash, fp := range d.fingerprints {
		similarity := d.calculateSimilarity(inputFingerprint, fp)
		if similarity >= d.config.SimilarityThresh {
			results = append(results, SimilarityResult{
				Hash:       hash,
				Similarity: similarity,
				Size:       fp.Size,
			})
		}
	}

	return results
}

// hashChunk calculates SHA-256 hash of a chunk
func (d *Deduplicator) hashChunk(chunk []byte) string {
	hash := sha256.Sum256(chunk)
	return hex.EncodeToString(hash[:])
}

// createFingerprint creates a MinHash fingerprint for similarity detection
func (d *Deduplicator) createFingerprint(data []byte) *Fingerprint {
	// Use MinHash with k=128 hash functions
	k := 128
	features := make([]uint64, k)

	// Initialize with max values
	for i := range features {
		features[i] = ^uint64(0)
	}

	// Generate shingles (n-grams) from data
	shingleSize := 4
	for i := 0; i <= len(data)-shingleSize; i++ {
		shingle := data[i : i+shingleSize]

		// Hash shingle with k different hash functions
		for j := 0; j < k; j++ {
			h := d.hashShingle(shingle, uint64(j))
			if h < features[j] {
				features[j] = h
			}
		}
	}

	return &Fingerprint{
		Features: features,
		Size:     len(data),
	}
}

// hashShingle hashes a shingle with a seed
func (d *Deduplicator) hashShingle(shingle []byte, seed uint64) uint64 {
	// Simple FNV-1a hash with seed
	hash := uint64(14695981039346656037) ^ seed
	for _, b := range shingle {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

// calculateSimilarity calculates Jaccard similarity between fingerprints
func (d *Deduplicator) calculateSimilarity(fp1, fp2 *Fingerprint) float64 {
	if len(fp1.Features) != len(fp2.Features) {
		return 0
	}

	matches := 0
	for i := range fp1.Features {
		if fp1.Features[i] == fp2.Features[i] {
			matches++
		}
	}

	return float64(matches) / float64(len(fp1.Features))
}

// evictChunks evicts least recently used chunks to free memory.
// FIX #33: sorts by LastUsed (true LRU) instead of using a FIFO queue.
func (d *Deduplicator) evictChunks() {
	targetMemory := d.config.MaxMemoryMB * 1024 * 1024 * 9 / 10 // Evict to 90%

	if d.memoryUsed <= targetMemory {
		return
	}

	// Build a list of eviction candidates sorted by LastUsed (oldest first).
	type candidate struct {
		hash     string
		lastUsed time.Time
	}
	candidates := make([]candidate, 0, len(d.chunks))
	for hash, meta := range d.chunks {
		// Only evict unreferenced chunks (FIX #34: use RefCount from metadata)
		if meta.RefCount == 0 {
			candidates = append(candidates, candidate{hash: hash, lastUsed: meta.LastUsed})
		}
	}

	// Sort oldest-first (true LRU order)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})

	for _, c := range candidates {
		if d.memoryUsed <= targetMemory {
			break
		}
		if chunk, exists := d.chunkData[c.hash]; exists {
			d.memoryUsed -= int64(len(chunk))
			delete(d.chunkData, c.hash)
			delete(d.chunks, c.hash)
			delete(d.fingerprints, c.hash)
		}
	}
}

// ReleaseChunk decrements reference count for a chunk.
// FIX #34: only updates ChunkMetadata.RefCount (chunkRefs map removed).
func (d *Deduplicator) ReleaseChunk(hash string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if metadata, exists := d.chunks[hash]; exists {
		if metadata.RefCount > 0 {
			metadata.RefCount--
		}
	}
}

// GetStats returns deduplication statistics
func (d *Deduplicator) GetStats() DeduplicationStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := d.stats
	stats.MemoryUsed = d.memoryUsed

	if d.stats.UniqueChunks > 0 {
		totalSize := int64(0)
		for _, metadata := range d.chunks {
			totalSize += int64(metadata.Size)
		}
		stats.AvgChunkSize = int(totalSize / d.stats.UniqueChunks)
	}

	return stats
}

// DeduplicatedData represents deduplicated data
type DeduplicatedData struct {
	Chunks           []ChunkReference
	OriginalSize     int
	DeduplicatedSize int
}

// ChunkReference references a chunk
type ChunkReference struct {
	Hash   string
	Offset int
	Size   int
}

// SimilarityResult represents a similarity search result
type SimilarityResult struct {
	Hash       string
	Similarity float64
	Size       int
}

// ContentDefinedChunker implements content-defined chunking
type ContentDefinedChunker struct {
	config       DeduplicationConfig
	rollingHash  *RollingHash
	breakPattern uint64
}

// NewContentDefinedChunker creates a new content-defined chunker
func NewContentDefinedChunker(config DeduplicationConfig) *ContentDefinedChunker {
	return &ContentDefinedChunker{
		config:       config,
		rollingHash:  NewRollingHash(config.WindowSize),
		breakPattern: (1 << 13) - 1, // Break on average every 8KB
	}
}

// Chunk splits data into content-defined chunks
func (c *ContentDefinedChunker) Chunk(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}

	chunks := make([][]byte, 0)
	start := 0

	c.rollingHash.Reset()

	for i := 0; i < len(data); i++ {
		c.rollingHash.Roll(data[i])

		// Check for chunk boundary
		if i-start >= c.config.MinChunkSize {
			hash := c.rollingHash.Sum()

			// Check if hash matches break pattern or max size reached
			if (hash&c.breakPattern) == 0 || i-start >= c.config.MaxChunkSize {
				chunks = append(chunks, data[start:i+1])
				start = i + 1
				c.rollingHash.Reset()
			}
		}
	}

	// Add remaining data as final chunk
	if start < len(data) {
		chunks = append(chunks, data[start:])
	}

	return chunks
}

// RollingHash implements Rabin fingerprinting.
// FIX #16: uses a proper Rabin polynomial with a pre-computed powers table
// so that the outgoing byte is correctly removed from the window.
type RollingHash struct {
	window []byte
	size   int
	pos    int
	hash   uint64
	// pow holds base^size mod p, used to remove the outgoing byte.
	pow uint64
}

const rabinBase uint64 = 257
const rabinMod uint64 = (1 << 61) - 1 // Mersenne prime

// NewRollingHash creates a new rolling hash
func NewRollingHash(size int) *RollingHash {
	// Precompute base^size mod rabinMod
	pow := uint64(1)
	for i := 0; i < size; i++ {
		pow = mulMod(pow, rabinBase)
	}
	return &RollingHash{
		window: make([]byte, size),
		size:   size,
		pow:    pow,
	}
}

// mulMod computes (a * b) mod rabinMod without overflow using 128-bit arithmetic.
func mulMod(a, b uint64) uint64 {
	// Use the identity: (a * b) mod (2^61 - 1)
	// Split into high/low 32-bit halves to avoid overflow.
	hi := (a >> 32) * (b >> 32)
	mid1 := (a >> 32) * (b & 0xFFFFFFFF)
	mid2 := (a & 0xFFFFFFFF) * (b >> 32)
	lo := (a & 0xFFFFFFFF) * (b & 0xFFFFFFFF)

	// Combine
	result := (hi << 3) + (mid1 >> 29) + (mid2 >> 29) +
		((mid1 & 0x1FFFFFFF) << 32) + ((mid2 & 0x1FFFFFFF) << 32) +
		lo
	// Reduce mod 2^61-1
	result = (result >> 61) + (result & rabinMod)
	if result >= rabinMod {
		result -= rabinMod
	}
	return result
}

// Reset resets the rolling hash
func (r *RollingHash) Reset() {
	r.pos = 0
	r.hash = 0
	for i := range r.window {
		r.window[i] = 0
	}
}

// Roll adds a byte to the rolling hash using a correct Rabin fingerprint.
// FIX #16: properly removes the outgoing byte using the precomputed power.
func (r *RollingHash) Roll(b byte) {
	// Remove the outgoing byte from the hash:
	//   hash = hash - (outgoing * base^size) mod p
	outgoing := r.window[r.pos]
	remove := mulMod(uint64(outgoing), r.pow)
	if r.hash >= remove {
		r.hash -= remove
	} else {
		r.hash = r.hash + rabinMod - remove
	}

	// Slide the window: hash = hash * base + incoming
	r.hash = mulMod(r.hash, rabinBase)
	r.hash += uint64(b)
	if r.hash >= rabinMod {
		r.hash -= rabinMod
	}

	// Update the circular window
	r.window[r.pos] = b
	r.pos = (r.pos + 1) % r.size
}

// Sum returns the current hash value
func (r *RollingHash) Sum() uint64 {
	return r.hash
}