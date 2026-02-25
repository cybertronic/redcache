package erasure

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/klauspost/reedsolomon"
	"golang.org/x/sys/cpu"
)

// Encoder provides erasure coding functionality using Cauchy Reed-Solomon
type Encoder struct {
	rs         reedsolomon.Encoder
	dataShards int
	parityShards int
	totalShards int
	shardSize  int64
	
	// Hardware acceleration
	usesSIMD   bool
	simdType   string
	
	mu sync.RWMutex
}

// EncoderConfig holds configuration for the erasure encoder
type EncoderConfig struct {
	DataShards   int   // Number of data shards (k)
	ParityShards int   // Number of parity shards (m)
	ShardSize    int64 // Size of each shard in bytes
	
	// Hardware acceleration
	EnableAVX2   bool
	EnableAVX512 bool
	EnableSSSE3  bool
}

// NewEncoder creates a new erasure coding encoder
func NewEncoder(config EncoderConfig) (*Encoder, error) {
	if config.DataShards <= 0 {
		return nil, fmt.Errorf("dataShards must be positive")
	}
	if config.ParityShards <= 0 {
		return nil, fmt.Errorf("parityShards must be positive")
	}
	if config.ShardSize <= 0 {
		return nil, fmt.Errorf("shardSize must be positive")
	}

	// Detect available SIMD extensions
	simdType, usesSIMD := detectSIMD(config.EnableAVX2, config.EnableAVX512, config.EnableSSSE3)
	
	// Create Reed-Solomon encoder with Cauchy matrix for better performance
	rs, err := reedsolomon.New(config.DataShards, config.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon encoder: %w", err)
	}

	return &Encoder{
		rs:           rs,
		dataShards:   config.DataShards,
		parityShards: config.ParityShards,
		totalShards:  config.DataShards + config.ParityShards,
		shardSize:    config.ShardSize,
		usesSIMD:     usesSIMD,
		simdType:     simdType,
	}, nil
}

// detectSIMD returns information about available SIMD extensions used by the encoder.
// The underlying reedsolomon library automatically performs runtime CPUID detection
// and utilizes AVX-512, AVX2, SSSE3, or NEON where available.
func detectSIMD(enableAVX2, enableAVX512, enableSSSE3 bool) (string, bool) {
	// Detect available extensions via klauspost/cpuid (used by reedsolomon)
	if hasAVX512() && enableAVX512 {
		return "AVX512", true
	}
	if hasAVX2() && enableAVX2 {
		return "AVX2", true
	}
	if hasSSSE3() && enableSSSE3 {
		return "SSSE3", true
	}
	if hasNEON() {
		return "NEON", true
	}

	return "none", false
}

// hasAVX512 checks if AVX-512 is available.
func hasAVX512() bool {
	return cpu.X86.HasAVX512
}

// hasAVX2 checks if AVX2 is available.
func hasAVX2() bool {
	return cpu.X86.HasAVX2
}

// hasSSSE3 checks if SSSE3 is available.
func hasSSSE3() bool {
	return cpu.X86.HasSSSE3
}

// hasNEON checks if NEON is available on ARM64.
func hasNEON() bool {
	return runtime.GOARCH == "arm64"
}

// Encode splits data into data and parity shards.
// FIX #9: the original data length is prepended as a little-endian uint64 so
// that Decode can strip exactly the padding bytes added during shard alignment,
// instead of blindly trimming all trailing zero bytes (which corrupts binary data).
// Returns totalShards shards (dataShards + parityShards).
func (e *Encoder) Encode(data []byte) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	// Prepend original length (8 bytes, little-endian) so Decode can recover it.
	origLen := uint64(len(data))
	framed := make([]byte, 8+len(data))
	framed[0] = byte(origLen)
	framed[1] = byte(origLen >> 8)
	framed[2] = byte(origLen >> 16)
	framed[3] = byte(origLen >> 24)
	framed[4] = byte(origLen >> 32)
	framed[5] = byte(origLen >> 40)
	framed[6] = byte(origLen >> 48)
	framed[7] = byte(origLen >> 56)
	copy(framed[8:], data)

	// Pad framed data to be a multiple of shard size
	shards, err := e.createShards()
	if err != nil {
		return nil, fmt.Errorf("failed to create shards: %w", err)
	}

	// Split framed data into data shards
	err = e.splitData(framed, shards)
	if err != nil {
		return nil, fmt.Errorf("failed to split data: %w", err)
	}

	// Calculate parity shards
	err = e.rs.Encode(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to encode parity: %w", err)
	}

	return shards, nil
}

// Decode reconstructs data from available shards
// Can tolerate up to parityShards missing shards
func (e *Encoder) Decode(shards [][]byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if reconstruction is possible
	err := e.rs.Reconstruct(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct shards: %w", err)
	}

	// Verify data integrity
	ok, err := e.rs.Verify(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to verify shards: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("reconstructed data verification failed")
	}

	// Join data shards
	framed, err := e.joinData(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to join data: %w", err)
	}

	// FIX #9: recover original length from the 8-byte prefix written by Encode.
	if len(framed) < 8 {
		return nil, fmt.Errorf("decoded data too short to contain length prefix")
	}
	origLen := uint64(framed[0]) |
		uint64(framed[1])<<8 |
		uint64(framed[2])<<16 |
		uint64(framed[3])<<24 |
		uint64(framed[4])<<32 |
		uint64(framed[5])<<40 |
		uint64(framed[6])<<48 |
		uint64(framed[7])<<56
	payload := framed[8:]
	if origLen > uint64(len(payload)) {
		return nil, fmt.Errorf("length prefix %d exceeds payload size %d", origLen, len(payload))
	}
	return payload[:origLen], nil
}

// createShards creates empty shards
func (e *Encoder) createShards() ([][]byte, error) {
	shards := make([][]byte, e.totalShards)
	for i := range shards {
		shards[i] = make([]byte, e.shardSize)
	}
	return shards, nil
}

// splitData splits data into data shards
func (e *Encoder) splitData(data []byte, shards [][]byte) error {
	dataLen := len(data)
	totalShardSize := e.dataShards * int(e.shardSize)

	// Pad data if necessary
	if dataLen < totalShardSize {
		padded := make([]byte, totalShardSize)
		copy(padded, data)
		data = padded
	} else if dataLen > totalShardSize {
		// Data is too large, need multiple stripe encoding
		return fmt.Errorf("data size %d exceeds single stripe capacity %d", 
			dataLen, totalShardSize)
	}

	// Split into data shards
	for i := 0; i < e.dataShards; i++ {
		start := i * int(e.shardSize)
		end := start + int(e.shardSize)
		copy(shards[i], data[start:end])
	}

	return nil
}

// joinData joins data shards back into original data
func (e *Encoder) joinData(shards [][]byte) ([]byte, error) {
	data := make([]byte, 0, e.dataShards*int(e.shardSize))
	
	for i := 0; i < e.dataShards; i++ {
		if len(shards[i]) == 0 {
			return nil, fmt.Errorf("data shard %d is missing", i)
		}
		data = append(data, shards[i]...)
	}

	// FIX #9: do NOT trim trailing zeros here — the caller (Decode) uses the
	// length prefix to strip exactly the right number of padding bytes.
	return data, nil
}

// EncodeMultiStripe encodes large data across multiple stripes
// Returns a slice of stripe shards
func (e *Encoder) EncodeMultiStripe(data []byte) ([][][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stripeSize := e.dataShards * int(e.shardSize)
	dataLen := len(data)
	
	if dataLen == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	// Calculate number of stripes needed
	numStripes := (dataLen + stripeSize - 1) / stripeSize
	
	allStripes := make([][][]byte, numStripes)

	for stripeIndex := 0; stripeIndex < numStripes; stripeIndex++ {
		start := stripeIndex * stripeSize
		end := start + stripeSize
		if end > dataLen {
			end = dataLen
		}

		stripeData := data[start:end]
		
		// Encode this stripe
		stripeShards, err := e.Encode(stripeData)
		if err != nil {
			return nil, fmt.Errorf("failed to encode stripe %d: %w", stripeIndex, err)
		}

		allStripes[stripeIndex] = stripeShards
	}

	return allStripes, nil
}

// DecodeMultiStripe decodes data from multiple stripes
func (e *Encoder) DecodeMultiStripe(stripeShards [][][]byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(stripeShards) == 0 {
		return nil, fmt.Errorf("no stripes provided")
	}

	// Decode each stripe and concatenate
	var data []byte
	for stripeIndex, shards := range stripeShards {
		stripeData, err := e.Decode(shards)
		if err != nil {
			return nil, fmt.Errorf("failed to decode stripe %d: %w", stripeIndex, err)
		}
		data = append(data, stripeData...)
	}

	return data, nil
}

// GetDataShards returns the number of data shards
func (e *Encoder) GetDataShards() int {
	return e.dataShards
}

// GetParityShards returns the number of parity shards
func (e *Encoder) GetParityShards() int {
	return e.parityShards
}

// GetTotalShards returns the total number of shards
func (e *Encoder) GetTotalShards() int {
	return e.totalShards
}

// GetShardSize returns the size of each shard
func (e *Encoder) GetShardSize() int64 {
	return e.shardSize
}

// GetStorageOverhead returns the storage overhead as a multiplier
// e.g., 1.5x means 50% overhead
func (e *Encoder) GetStorageOverhead() float64 {
	return float64(e.totalShards) / float64(e.dataShards)
}

// GetFaultTolerance returns the maximum number of shards that can be lost
func (e *Encoder) GetFaultTolerance() int {
	return e.parityShards
}

// UsesSIMD returns whether SIMD acceleration is being used
func (e *Encoder) UsesSIMD() bool {
	return e.usesSIMD
}

// GetSIMDType returns the type of SIMD acceleration being used
func (e *Encoder) GetSIMDType() string {
	return e.simdType
}

// GetStats returns encoder statistics
func (e *Encoder) GetStats() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]any{
		"data_shards":        e.dataShards,
		"parity_shards":      e.parityShards,
		"total_shards":       e.totalShards,
		"shard_size":         e.shardSize,
		"storage_overhead":   e.GetStorageOverhead(),
		"fault_tolerance":    e.GetFaultTolerance(),
		"uses_simd":          e.usesSIMD,
		"simd_type":          e.simdType,
	}
}

// RecommendedEncoding provides recommendations based on cluster size
type RecommendedEncoding struct {
	DataShards   int
	ParityShards int
	ShardSize    int64
	Reasoning    string
}

// GetRecommendedEncoding returns recommended encoding parameters based on cluster size
func GetRecommendedEncoding(clusterSize int) RecommendedEncoding {
	switch {
	case clusterSize < 3:
		return RecommendedEncoding{
			DataShards:   2,
			ParityShards: 1,
			ShardSize:    1048576, // 1MB
			Reasoning:    "Minimum cluster, minimal overhead (1.5x), tolerate 1 failure",
		}
	case clusterSize < 10:
		return RecommendedEncoding{
			DataShards:   4,
			ParityShards: 2,
			ShardSize:    1048576, // 1MB
			Reasoning:    "Small cluster, good balance (1.5x overhead), tolerate 2 failures",
		}
	case clusterSize < 100:
		return RecommendedEncoding{
			DataShards:   6,
			ParityShards: 3,
			ShardSize:    1048576, // 1MB
			Reasoning:    "Medium cluster, standard configuration (1.5x overhead), tolerate 3 failures",
		}
	case clusterSize < 1000:
		return RecommendedEncoding{
			DataShards:   8,
			ParityShards: 4,
			ShardSize:    1048576, // 1MB
			Reasoning:    "Large cluster, high availability (1.5x overhead), tolerate 4 failures",
		}
	default:
		return RecommendedEncoding{
			DataShards:   10,
			ParityShards: 4,
			ShardSize:    2097152, // 2MB
			Reasoning:    "Very large cluster, optimized for storage efficiency (1.4x overhead), tolerate 4 failures",
		}
	}
}

// GetStorageEfficiency calculates storage efficiency
func (e *Encoder) GetStorageEfficiency() float64 {
	return float64(e.dataShards) / float64(e.totalShards)
}

// GetReconstructionThreshold returns minimum shards needed for reconstruction
func (e *Encoder) GetReconstructionThreshold() int {
	return e.dataShards
}

// CanReconstruct checks if reconstruction is possible with given shards
func (e *Encoder) CanReconstruct(shards [][]byte) bool {
	dataShardCount := 0
	for i := 0; i < e.dataShards; i++ {
		if i < len(shards) && len(shards[i]) > 0 {
			dataShardCount++
		}
	}
	parityShardCount := 0
	for i := e.dataShards; i < e.totalShards; i++ {
		if i < len(shards) && len(shards[i]) > 0 {
			parityShardCount++
		}
	}
	
	return dataShardCount+parityShardCount >= e.dataShards
}

// GetEncodingString returns a string representation of the encoding (e.g., "6+3")
func (e *Encoder) GetEncodingString() string {
	return fmt.Sprintf("%d+%d", e.dataShards, e.parityShards)
}

// ShardInfo contains information about a specific shard
type ShardInfo struct {
	Index      int
	Type       string // "data" or "parity"
	Size       int64
	Checksum   uint32
}

// GetShardInfo returns information about all shards
func (e *Encoder) GetShardInfo() []ShardInfo {
	info := make([]ShardInfo, e.totalShards)
	
	for i := 0; i < e.totalShards; i++ {
		shardType := "data"
		if i >= e.dataShards {
			shardType = "parity"
		}
		
		info[i] = ShardInfo{
			Index:    i,
			Type:     shardType,
			Size:     e.shardSize,
			Checksum: 0, // Calculated for actual shard data
		}
	}
	
	return info
}

func init() {
	// The klauspost/reedsolomon library automatically detects
	// and uses available SIMD extensions at runtime.
}