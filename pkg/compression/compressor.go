package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// CompressionAlgorithm defines the compression algorithm
type CompressionAlgorithm int

const (
	AlgorithmNone CompressionAlgorithm = iota
	AlgorithmGzip
	AlgorithmZstd
	AlgorithmLZ4
	AlgorithmAuto // Automatically select best algorithm
)

// CompressionLevel defines the compression level
type CompressionLevel int

const (
	LevelFastest CompressionLevel = iota
	LevelDefault
	LevelBest
)

// CompressorConfig configures the compression engine
type CompressorConfig struct {
	Algorithm        CompressionAlgorithm
	Level            CompressionLevel
	MinSize          int64   // Minimum size to compress (bytes)
	CompressionRatio float64 // Minimum compression ratio to keep compressed
	EnableAdaptive   bool    // Enable adaptive algorithm selection
	BlockSize        int     // Block size for streaming compression
}

// Compressor handles data compression and decompression
type Compressor struct {
	config CompressorConfig
	mu     sync.RWMutex
	
	// Encoder pools for reuse
	gzipPool *sync.Pool
	zstdPool *sync.Pool
	lz4Pool  *sync.Pool
	
	// Statistics
	stats CompressionStats
	
	// Adaptive selection
	algorithmScores map[CompressionAlgorithm]*AlgorithmScore
}

// AlgorithmScore tracks performance of each algorithm
type AlgorithmScore struct {
	TotalBytes        int64
	CompressedBytes   int64
	CompressionTime   int64 // nanoseconds
	DecompressionTime int64 // nanoseconds
	Samples           int64
	AvgRatio          float64
	AvgSpeed          float64 // MB/s
}

// CompressionStats contains compression statistics
type CompressionStats struct {
	TotalCompressed      int64
	TotalDecompressed    int64
	BytesSaved           int64
	AvgRatio             float64
	AvgCompressionTime   int64
	AvgDecompressionTime int64
}

// NewCompressor creates a new compressor
func NewCompressor(config CompressorConfig) *Compressor {
	if config.MinSize == 0 {
		config.MinSize = 1024 // 1KB minimum
	}
	if config.CompressionRatio == 0 {
		config.CompressionRatio = 0.9 // Keep if compressed to 90% or less
	}
	if config.BlockSize == 0 {
		config.BlockSize = 64 * 1024 // 64KB blocks
	}
	
	c := &Compressor{
		config:          config,
		algorithmScores: make(map[CompressionAlgorithm]*AlgorithmScore),
	}
	
	// Initialize encoder pools
	c.gzipPool = &sync.Pool{
		New: func() any {
			level := gzip.DefaultCompression
			switch config.Level {
			case LevelFastest:
				level = gzip.BestSpeed
			case LevelBest:
				level = gzip.BestCompression
			}
			w, _ := gzip.NewWriterLevel(nil, level)
			return w
		},
	}
	
	c.zstdPool = &sync.Pool{
		New: func() any {
			level := zstd.SpeedDefault
			switch config.Level {
			case LevelFastest:
				level = zstd.SpeedFastest
			case LevelBest:
				level = zstd.SpeedBestCompression
			}
			w, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
			return w
		},
	}
	
	c.lz4Pool = &sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	
	// Initialize algorithm scores
	c.algorithmScores[AlgorithmGzip] = &AlgorithmScore{}
	c.algorithmScores[AlgorithmZstd] = &AlgorithmScore{}
	c.algorithmScores[AlgorithmLZ4] = &AlgorithmScore{}
	
	return c
}

// Compress compresses data using the configured algorithm
func (c *Compressor) Compress(ctx context.Context, data []byte) (*CompressedData, error) {
	// Skip compression for small data
	if int64(len(data)) < c.config.MinSize {
		return &CompressedData{
			Data:       data,
			Algorithm:  AlgorithmNone,
			Original:   len(data),
			Compressed: len(data),
		}, nil
	}
	
	// Select algorithm
	algorithm := c.config.Algorithm
	if algorithm == AlgorithmAuto {
		algorithm = c.selectBestAlgorithm(data)
	}
	
	// Compress
	startTime := timeNow()
	compressed, err := c.compressWithAlgorithm(data, algorithm)
	compressionTime := timeSince(startTime)
	
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}
	
	// Check compression ratio
	ratio := float64(len(compressed)) / float64(len(data))
	if ratio > c.config.CompressionRatio {
		// Compression not effective, return original
		return &CompressedData{
			Data:       data,
			Algorithm:  AlgorithmNone,
			Original:   len(data),
			Compressed: len(data),
		}, nil
	}
	
	// Update statistics
	c.updateCompressionStats(algorithm, len(data), len(compressed), compressionTime)
	
	return &CompressedData{
		Data:       compressed,
		Algorithm:  algorithm,
		Original:   len(data),
		Compressed: len(compressed),
	}, nil
}

// Decompress decompresses data
func (c *Compressor) Decompress(ctx context.Context, compressed *CompressedData) ([]byte, error) {
	if compressed.Algorithm == AlgorithmNone {
		return compressed.Data, nil
	}
	
	startTime := timeNow()
	data, err := c.decompressWithAlgorithm(compressed.Data, compressed.Algorithm)
	decompressionTime := timeSince(startTime)
	
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}
	
	// Update statistics
	c.updateDecompressionStats(compressed.Algorithm, decompressionTime)
	
	return data, nil
}

// compressWithAlgorithm compresses data with specific algorithm
func (c *Compressor) compressWithAlgorithm(data []byte, algorithm CompressionAlgorithm) ([]byte, error) {
	switch algorithm {
	case AlgorithmGzip:
		return c.compressGzip(data)
	case AlgorithmZstd:
		return c.compressZstd(data)
	case AlgorithmLZ4:
		return c.compressLZ4(data)
	default:
		return data, nil
	}
}

// decompressWithAlgorithm decompresses data with specific algorithm
func (c *Compressor) decompressWithAlgorithm(data []byte, algorithm CompressionAlgorithm) ([]byte, error) {
	switch algorithm {
	case AlgorithmGzip:
		return c.decompressGzip(data)
	case AlgorithmZstd:
		return c.decompressZstd(data)
	case AlgorithmLZ4:
		return c.decompressLZ4(data)
	default:
		return data, nil
	}
}

// compressGzip compresses data using gzip.
// FIX #14: w.Close() is always called via defer, even if w.Write() fails.
func (c *Compressor) compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := c.gzipPool.Get().(*gzip.Writer)

	w.Reset(&buf)
	_, writeErr := w.Write(data)
	closeErr := w.Close()

	// Return the writer to the pool only when it is in a clean state.
	// If either Write or Close failed the writer may be in a dirty state;
	// discard it so the pool always contains healthy writers.
	if writeErr == nil && closeErr == nil {
		c.gzipPool.Put(w)
	}

	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, closeErr
	}

	return buf.Bytes(), nil
}

// decompressGzip decompresses gzip data
func (c *Compressor) decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	
	return io.ReadAll(r)
}

// compressZstd compresses data using zstd
func (c *Compressor) compressZstd(data []byte) ([]byte, error) {
	w := c.zstdPool.Get().(*zstd.Encoder)
	defer c.zstdPool.Put(w)
	
	return w.EncodeAll(data, make([]byte, 0, len(data))), nil
}

// decompressZstd decompresses zstd data
func (c *Compressor) decompressZstd(data []byte) ([]byte, error) {
	d, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	
	return d.DecodeAll(data, nil)
}

// compressLZ4 compresses data using LZ4.
// FIX #14: same pattern as compressGzip — always close, discard dirty writers.
func (c *Compressor) compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := c.lz4Pool.Get().(*lz4.Writer)

	w.Reset(&buf)
	_, writeErr := w.Write(data)
	closeErr := w.Close()

	if writeErr == nil && closeErr == nil {
		c.lz4Pool.Put(w)
	}

	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, closeErr
	}

	return buf.Bytes(), nil
}

// decompressLZ4 decompresses LZ4 data
func (c *Compressor) decompressLZ4(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(r)
}

// selectBestAlgorithm selects the best algorithm based on data characteristics.
// FIX #4: uses math.Log2 (stdlib) instead of the broken hand-rolled log().
func (c *Compressor) selectBestAlgorithm(data []byte) CompressionAlgorithm {
	if !c.config.EnableAdaptive {
		return AlgorithmZstd // Default to Zstd
	}
	
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Calculate entropy to estimate compressibility
	entropy := c.calculateEntropy(data)
	
	// High entropy (>7.5) - data is likely already compressed or random
	if entropy > 7.5 {
		return AlgorithmLZ4 // Fastest for incompressible data
	}
	
	// Low entropy (<5.0) - highly compressible
	if entropy < 5.0 {
		return AlgorithmZstd // Best compression
	}
	
	// Medium entropy - balance speed and compression
	// Select based on historical performance
	bestAlgorithm := AlgorithmZstd
	bestScore := 0.0
	
	for algo, score := range c.algorithmScores {
		if score.Samples == 0 {
			continue
		}
		
		// Score = compression ratio * speed
		algoScore := (1.0 - score.AvgRatio) * score.AvgSpeed
		if algoScore > bestScore {
			bestScore = algoScore
			bestAlgorithm = algo
		}
	}
	
	return bestAlgorithm
}

// calculateEntropy calculates Shannon entropy of data.
// FIX #4: uses math.Log2 from the standard library.
func (c *Compressor) calculateEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	
	// Count byte frequencies
	freq := make([]int, 256)
	for _, b := range data {
		freq[b]++
	}
	
	// Calculate entropy using math.Log2
	entropy := 0.0
	length := float64(len(data))
	
	for _, count := range freq {
		if count == 0 {
			continue
		}
		p := float64(count) / length
		entropy -= p * math.Log2(p) // FIX #4: math.Log2 handles 0 < p <= 1 correctly
	}
	
	return entropy
}

// updateCompressionStats updates compression statistics.
// FIX #15: AvgRatio is now bytes/bytes (dimensionally correct).
func (c *Compressor) updateCompressionStats(algorithm CompressionAlgorithm, original, compressed int, duration int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.stats.TotalCompressed++
	c.stats.BytesSaved += int64(original - compressed)
	
	// Update algorithm-specific stats
	score := c.algorithmScores[algorithm]
	score.TotalBytes += int64(original)
	score.CompressedBytes += int64(compressed)
	score.CompressionTime += duration
	score.Samples++
	
	// FIX #15: AvgRatio = compressed_bytes / original_bytes (both in bytes, dimensionally correct)
	score.AvgRatio = float64(score.CompressedBytes) / float64(score.TotalBytes)
	avgTime := float64(score.CompressionTime) / float64(score.Samples)
	if avgTime > 0 {
		score.AvgSpeed = float64(score.TotalBytes) / (avgTime / 1e9) / 1024 / 1024 // MB/s
	}
	
	// FIX #15: global AvgRatio = total_compressed_bytes / total_original_bytes
	if c.stats.TotalCompressed > 0 {
		totalOriginal := int64(0)
		totalCompressed := int64(0)
		for _, s := range c.algorithmScores {
			totalOriginal += s.TotalBytes
			totalCompressed += s.CompressedBytes
		}
		if totalOriginal > 0 {
			c.stats.AvgRatio = float64(totalCompressed) / float64(totalOriginal)
		}
		c.stats.AvgCompressionTime = duration
	}
}

// updateDecompressionStats updates decompression statistics
func (c *Compressor) updateDecompressionStats(algorithm CompressionAlgorithm, duration int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.stats.TotalDecompressed++
	
	score := c.algorithmScores[algorithm]
	score.DecompressionTime += duration
	
	c.stats.AvgDecompressionTime = duration
}

// GetStats returns compression statistics
func (c *Compressor) GetStats() CompressionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.stats
}

// GetAlgorithmStats returns per-algorithm statistics
func (c *Compressor) GetAlgorithmStats() map[CompressionAlgorithm]*AlgorithmScore {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	stats := make(map[CompressionAlgorithm]*AlgorithmScore)
	for algo, score := range c.algorithmScores {
		scoreCopy := *score
		stats[algo] = &scoreCopy
	}
	
	return stats
}

// CompressedData represents compressed data with metadata
type CompressedData struct {
	Data       []byte
	Algorithm  CompressionAlgorithm
	Original   int
	Compressed int
}

// Helper functions for time measurement
var (
	timeNow   = func() int64 { return time.Now().UnixNano() }
	timeSince = func(start int64) int64 { return time.Now().UnixNano() - start }
)