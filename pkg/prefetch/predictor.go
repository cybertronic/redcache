package prefetch

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AccessPattern represents a detected access pattern
type AccessPattern int

const (
	PatternSequential AccessPattern = iota
	PatternRandom
	PatternStrided
	PatternTemporal
)

// PredictorConfig configures the ML-based predictor
type PredictorConfig struct {
	WindowSize        int           // Number of recent accesses to consider
	PredictionDepth   int           // How many future accesses to predict
	ConfidenceThresh  float64       // Minimum confidence to trigger prefetch
	LearningRate      float64       // Learning rate for pattern adaptation
	DecayFactor       float64       // Decay factor for old patterns
	MaxPrefetchSize   int64         // Maximum bytes to prefetch
	PrefetchAhead     int           // Number of objects to prefetch ahead
	AdaptiveThreshold bool          // Enable adaptive confidence threshold
	DecayInterval     int           // How many RecordAccess calls between decay runs (FIX #35)
}

// AccessRecord tracks a single access
type AccessRecord struct {
	Key       string
	Timestamp time.Time
	Size      int64
	Offset    int64
	Length    int64
}

// PatternStats tracks statistics for a pattern
type PatternStats struct {
	Count      int
	Hits       int
	Misses     int
	Confidence float64
	LastSeen   time.Time
}

// Predictor implements ML-based access prediction
type Predictor struct {
	config  PredictorConfig
	mu      sync.RWMutex
	history []AccessRecord

	// Pattern detection
	patterns      map[AccessPattern]*PatternStats
	sequenceMap   map[string][]string      // key -> next keys
	strideMap     map[string]int64         // key -> stride
	temporalMap   map[string]time.Duration // key -> typical interval

	// Markov chain for prediction
	transitionMatrix map[string]map[string]float64

	// Performance tracking
	predictions   int64
	hits          int64
	falsePositive int64

	// Adaptive learning
	recentAccuracy []float64
	adaptiveConf   float64

	// FIX #35: counter to throttle decayPatterns calls
	accessCount int
}

// NewPredictor creates a new ML-based predictor
func NewPredictor(config PredictorConfig) *Predictor {
	if config.WindowSize == 0 {
		config.WindowSize = 1000
	}
	if config.PredictionDepth == 0 {
		config.PredictionDepth = 5
	}
	if config.ConfidenceThresh == 0 {
		config.ConfidenceThresh = 0.7
	}
	if config.LearningRate == 0 {
		config.LearningRate = 0.1
	}
	if config.DecayFactor == 0 {
		config.DecayFactor = 0.95
	}
	if config.PrefetchAhead == 0 {
		config.PrefetchAhead = 3
	}
	// FIX #35: default decay interval — run decay every 100 accesses
	if config.DecayInterval == 0 {
		config.DecayInterval = 100
	}

	return &Predictor{
		config:           config,
		history:          make([]AccessRecord, 0, config.WindowSize),
		patterns:         make(map[AccessPattern]*PatternStats),
		sequenceMap:      make(map[string][]string),
		strideMap:        make(map[string]int64),
		temporalMap:      make(map[string]time.Duration),
		transitionMatrix: make(map[string]map[string]float64),
		recentAccuracy:   make([]float64, 0, 100),
		adaptiveConf:     config.ConfidenceThresh,
	}
}

// RecordAccess records a new access and updates patterns.
// FIX #35: decayPatterns is called only every config.DecayInterval accesses
// instead of on every single call, reducing O(n²) work to amortised O(n).
func (p *Predictor) RecordAccess(ctx context.Context, record AccessRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Add to history
	p.history = append(p.history, record)
	if len(p.history) > p.config.WindowSize {
		p.history = p.history[1:]
	}

	// Update patterns
	p.updateSequencePattern(record)
	p.updateStridePattern(record)
	p.updateTemporalPattern(record)
	p.updateTransitionMatrix(record)

	// FIX #35: decay only every DecayInterval accesses
	p.accessCount++
	if p.accessCount >= p.config.DecayInterval {
		p.decayPatterns()
		p.accessCount = 0
	}
}

// Predict predicts the next likely accesses
func (p *Predictor) Predict(ctx context.Context, currentKey string) []PredictionResult {
	p.mu.RLock()
	defer p.mu.RUnlock()

	predictions := make([]PredictionResult, 0, p.config.PredictionDepth)

	// Detect dominant pattern
	pattern := p.detectPattern(currentKey)

	switch pattern {
	case PatternSequential:
		predictions = p.predictSequential(currentKey)
	case PatternStrided:
		predictions = p.predictStrided(currentKey)
	case PatternTemporal:
		predictions = p.predictTemporal(currentKey)
	default:
		predictions = p.predictMarkov(currentKey)
	}

	// Filter by confidence threshold
	threshold := p.adaptiveConf
	if !p.config.AdaptiveThreshold {
		threshold = p.config.ConfidenceThresh
	}

	filtered := make([]PredictionResult, 0, len(predictions))
	for _, pred := range predictions {
		if pred.Confidence >= threshold {
			filtered = append(filtered, pred)
		}
	}

	return filtered
}

// PredictionResult represents a predicted access
type PredictionResult struct {
	Key        string
	Confidence float64
	Pattern    AccessPattern
	Priority   int
}

// detectPattern detects the dominant access pattern
func (p *Predictor) detectPattern(key string) AccessPattern {
	if len(p.history) < 3 {
		return PatternRandom
	}

	// Check for sequential pattern
	if p.isSequential(key) {
		return PatternSequential
	}

	// Check for strided pattern
	if p.isStrided(key) {
		return PatternStrided
	}

	// Check for temporal pattern
	if p.isTemporal(key) {
		return PatternTemporal
	}

	return PatternRandom
}

// isSequential checks if access pattern is sequential
func (p *Predictor) isSequential(key string) bool {
	sequences, exists := p.sequenceMap[key]
	if !exists || len(sequences) == 0 {
		return false
	}

	// Check if there's a clear next key
	counts := make(map[string]int)
	for _, next := range sequences {
		counts[next]++
	}

	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}

	// If one key appears >70% of the time, it's sequential
	return float64(maxCount)/float64(len(sequences)) > 0.7
}

// isStrided checks if access pattern has a stride
func (p *Predictor) isStrided(key string) bool {
	stride, exists := p.strideMap[key]
	return exists && stride > 0
}

// isTemporal checks if access pattern is temporal
func (p *Predictor) isTemporal(key string) bool {
	_, exists := p.temporalMap[key]
	return exists
}

// predictSequential predicts sequential accesses
func (p *Predictor) predictSequential(key string) []PredictionResult {
	sequences, exists := p.sequenceMap[key]
	if !exists || len(sequences) == 0 {
		return nil
	}

	// Count occurrences
	counts := make(map[string]int)
	for _, next := range sequences {
		counts[next]++
	}

	// Sort by frequency
	results := make([]PredictionResult, 0, p.config.PredictionDepth)
	for nextKey, count := range counts {
		confidence := float64(count) / float64(len(sequences))
		if len(results) < p.config.PredictionDepth {
			results = append(results, PredictionResult{
				Key:        nextKey,
				Confidence: confidence,
				Pattern:    PatternSequential,
				Priority:   count,
			})
		}
	}

	return results
}

// predictStrided predicts strided accesses.
// FIX #18: generates actual strided keys by parsing the numeric suffix of the
// current key and adding multiples of the detected stride.
func (p *Predictor) predictStrided(key string) []PredictionResult {
	stride, exists := p.strideMap[key]
	if !exists || stride == 0 {
		return nil
	}

	// Parse the numeric component from the key so we can compute future keys.
	// Keys are expected to have a numeric suffix (e.g. "object-42", "chunk_7").
	prefix, baseNum, ok := splitKeyNumericSuffix(key)
	if !ok {
		// Cannot compute strided keys without a numeric component.
		return nil
	}

	results := make([]PredictionResult, 0, p.config.PrefetchAhead)

	for i := 1; i <= p.config.PrefetchAhead; i++ {
		nextNum := baseNum + stride*int64(i)
		nextKey := fmt.Sprintf("%s%d", prefix, nextNum)
		results = append(results, PredictionResult{
			Key:        nextKey, // FIX #18: actual predicted key, not the current key
			Confidence: 0.8,
			Pattern:    PatternStrided,
			Priority:   p.config.PrefetchAhead - i + 1,
		})
	}

	return results
}

// splitKeyNumericSuffix splits a key like "prefix-42" into ("prefix-", 42, true).
// Returns ("", 0, false) if no numeric suffix is found.
func splitKeyNumericSuffix(key string) (prefix string, num int64, ok bool) {
	// Find the last run of digits at the end of the key.
	i := len(key)
	for i > 0 && key[i-1] >= '0' && key[i-1] <= '9' {
		i--
	}
	if i == len(key) {
		// No trailing digits.
		return "", 0, false
	}
	n, err := strconv.ParseInt(key[i:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return key[:i], n, true
}

// predictTemporal predicts temporal accesses
func (p *Predictor) predictTemporal(key string) []PredictionResult {
	_, exists := p.temporalMap[key]
	if !exists {
		return nil
	}

	// Find keys likely to be accessed based on time
	now := time.Now()
	results := make([]PredictionResult, 0)

	for k, lastInterval := range p.temporalMap {
		if k == key {
			continue
		}

		// Check if this key is likely to be accessed soon
		for _, record := range p.history {
			if record.Key == k {
				timeSince := now.Sub(record.Timestamp)
				if timeSince >= lastInterval*9/10 && timeSince <= lastInterval*11/10 {
					results = append(results, PredictionResult{
						Key:        k,
						Confidence: 0.75,
						Pattern:    PatternTemporal,
						Priority:   1,
					})
				}
				break
			}
		}
	}

	return results
}

// predictMarkov uses Markov chain for prediction
func (p *Predictor) predictMarkov(key string) []PredictionResult {
	transitions, exists := p.transitionMatrix[key]
	if !exists || len(transitions) == 0 {
		return nil
	}

	results := make([]PredictionResult, 0, len(transitions))
	for nextKey, prob := range transitions {
		if len(results) < p.config.PredictionDepth {
			results = append(results, PredictionResult{
				Key:        nextKey,
				Confidence: prob,
				Pattern:    PatternRandom,
				Priority:   int(prob * 100),
			})
		}
	}

	return results
}

// updateSequencePattern updates sequential pattern detection
func (p *Predictor) updateSequencePattern(record AccessRecord) {
	if len(p.history) < 2 {
		return
	}

	prevKey := p.history[len(p.history)-2].Key
	if _, exists := p.sequenceMap[prevKey]; !exists {
		p.sequenceMap[prevKey] = make([]string, 0)
	}
	p.sequenceMap[prevKey] = append(p.sequenceMap[prevKey], record.Key)

	// Keep only recent sequences
	if len(p.sequenceMap[prevKey]) > 100 {
		p.sequenceMap[prevKey] = p.sequenceMap[prevKey][1:]
	}
}

// updateStridePattern updates stride pattern detection
func (p *Predictor) updateStridePattern(record AccessRecord) {
	if len(p.history) < 3 {
		return
	}

	// Look for numeric patterns in keys
	prevRecord := p.history[len(p.history)-2]
	if record.Offset > 0 && prevRecord.Offset > 0 {
		stride := record.Offset - prevRecord.Offset
		if stride > 0 {
			p.strideMap[prevRecord.Key] = stride
		}
	}

	// Also detect stride from numeric key suffixes (FIX #18 support)
	_, curNum, curOK := splitKeyNumericSuffix(record.Key)
	_, prevNum, prevOK := splitKeyNumericSuffix(prevRecord.Key)
	if curOK && prevOK {
		stride := curNum - prevNum
		if stride > 0 {
			p.strideMap[prevRecord.Key] = stride
		}
	}
}

// updateTemporalPattern updates temporal pattern detection
func (p *Predictor) updateTemporalPattern(record AccessRecord) {
	// Find previous access to same key
	for i := len(p.history) - 2; i >= 0; i-- {
		if p.history[i].Key == record.Key {
			interval := record.Timestamp.Sub(p.history[i].Timestamp)
			p.temporalMap[record.Key] = interval
			break
		}
	}
}

// updateTransitionMatrix updates Markov transition probabilities
func (p *Predictor) updateTransitionMatrix(record AccessRecord) {
	if len(p.history) < 2 {
		return
	}

	prevKey := p.history[len(p.history)-2].Key

	if _, exists := p.transitionMatrix[prevKey]; !exists {
		p.transitionMatrix[prevKey] = make(map[string]float64)
	}

	// Update transition probability with learning rate
	currentProb := p.transitionMatrix[prevKey][record.Key]
	p.transitionMatrix[prevKey][record.Key] = currentProb + p.config.LearningRate*(1.0-currentProb)

	// Normalize probabilities
	total := 0.0
	for _, prob := range p.transitionMatrix[prevKey] {
		total += prob
	}
	if total > 0 {
		for k := range p.transitionMatrix[prevKey] {
			p.transitionMatrix[prevKey][k] /= total
		}
	}
}

// decayPatterns applies decay to old patterns.
// FIX #35: this is now called only every DecayInterval accesses (see RecordAccess).
func (p *Predictor) decayPatterns() {
	for key, transitions := range p.transitionMatrix {
		for nextKey := range transitions {
			p.transitionMatrix[key][nextKey] *= p.config.DecayFactor

			// Remove very low probabilities
			if p.transitionMatrix[key][nextKey] < 0.01 {
				delete(p.transitionMatrix[key], nextKey)
			}
		}

		// Remove empty entries
		if len(p.transitionMatrix[key]) == 0 {
			delete(p.transitionMatrix, key)
		}
	}
}

// RecordPredictionOutcome records whether a prediction was correct
func (p *Predictor) RecordPredictionOutcome(predicted string, actual string, hit bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.predictions++
	if hit {
		p.hits++
	} else {
		p.falsePositive++
	}

	// Update adaptive confidence threshold
	if p.config.AdaptiveThreshold {
		accuracy := float64(p.hits) / float64(p.predictions)
		p.recentAccuracy = append(p.recentAccuracy, accuracy)
		if len(p.recentAccuracy) > 100 {
			p.recentAccuracy = p.recentAccuracy[1:]
		}

		// Adjust confidence threshold based on recent accuracy
		avgAccuracy := 0.0
		for _, acc := range p.recentAccuracy {
			avgAccuracy += acc
		}
		avgAccuracy /= float64(len(p.recentAccuracy))

		// If accuracy is low, increase threshold; if high, decrease it
		if avgAccuracy < 0.7 {
			p.adaptiveConf = math.Min(0.9, p.adaptiveConf+0.01)
		} else if avgAccuracy > 0.85 {
			p.adaptiveConf = math.Max(0.5, p.adaptiveConf-0.01)
		}
	}
}

// GetStats returns predictor statistics
func (p *Predictor) GetStats() PredictorStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	accuracy := 0.0
	if p.predictions > 0 {
		accuracy = float64(p.hits) / float64(p.predictions)
	}

	return PredictorStats{
		TotalPredictions: p.predictions,
		Hits:             p.hits,
		FalsePositives:   p.falsePositive,
		Accuracy:         accuracy,
		AdaptiveConf:     p.adaptiveConf,
		HistorySize:      len(p.history),
		PatternCount:     len(p.transitionMatrix),
	}
}

// PredictorStats contains predictor statistics
type PredictorStats struct {
	TotalPredictions int64
	Hits             int64
	FalsePositives   int64
	Accuracy         float64
	AdaptiveConf     float64
	HistorySize      int
	PatternCount     int
}

// ensure strings import is used (splitKeyNumericSuffix uses strings indirectly via fmt)
var _ = strings.Contains