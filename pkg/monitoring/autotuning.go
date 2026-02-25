package monitoring

import (
	"context"
	"math"
	"sync"
	"time"
)

// AutoTuner implements automated performance tuning
type AutoTuner struct {
	config AutoTunerConfig
	mu     sync.RWMutex
	
	// Tunable parameters
	parameters map[string]*TunableParameter
	
	// Performance metrics
	metrics *PerformanceMetrics
	
	// Optimization history
	history []OptimizationResult
	
	// Learning
	learningRate float64
	
	// Statistics
	stats AutoTuningStats
}

// AutoTunerConfig configures the auto-tuner
type AutoTunerConfig struct {
	TuningInterval    time.Duration
	LearningRate      float64
	ExplorationRate   float64
	MaxIterations     int
	ConvergenceThresh float64
	EnableAdaptive    bool
}

// TunableParameter represents a tunable parameter
type TunableParameter struct {
	Name         string
	CurrentValue float64
	MinValue     float64
	MaxValue     float64
	StepSize     float64
	Impact       float64 // Impact on performance
	History      []float64
	BestValue    float64
	BestScore    float64
}

// PerformanceMetrics tracks performance metrics
type PerformanceMetrics struct {
	Throughput    float64
	Latency       float64
	HitRate       float64
	CPUUsage      float64
	MemoryUsage   float64
	NetworkIO     float64
	OverallScore  float64
	Timestamp     time.Time
}

// OptimizationResult represents an optimization result
type OptimizationResult struct {
	Iteration   int
	Parameters  map[string]float64
	Metrics     PerformanceMetrics
	Score       float64
	Improvement float64
	Timestamp   time.Time
}

// AutoTuningStats contains auto-tuning statistics
type AutoTuningStats struct {
	TotalIterations   int64
	Improvements      int64
	Degradations      int64
	BestScore         float64
	AvgImprovement    float64
	ConvergedParams   int
	LastTuningTime    time.Time
}

// NewAutoTuner creates a new auto-tuner
func NewAutoTuner(config AutoTunerConfig) *AutoTuner {
	if config.TuningInterval == 0 {
		config.TuningInterval = 5 * time.Minute
	}
	if config.LearningRate == 0 {
		config.LearningRate = 0.1
	}
	if config.ExplorationRate == 0 {
		config.ExplorationRate = 0.2
	}
	if config.MaxIterations == 0 {
		config.MaxIterations = 100
	}
	if config.ConvergenceThresh == 0 {
		config.ConvergenceThresh = 0.01
	}
	
	return &AutoTuner{
		config:       config,
		parameters:   make(map[string]*TunableParameter),
		metrics:      &PerformanceMetrics{},
		history:      make([]OptimizationResult, 0),
		learningRate: config.LearningRate,
	}
}

// RegisterParameter registers a tunable parameter
func (at *AutoTuner) RegisterParameter(name string, current, min, max, step float64) {
	at.mu.Lock()
	defer at.mu.Unlock()
	
	param := &TunableParameter{
		Name:         name,
		CurrentValue: current,
		MinValue:     min,
		MaxValue:     max,
		StepSize:     step,
		History:      make([]float64, 0),
		BestValue:    current,
		BestScore:    0,
	}
	
	at.parameters[name] = param
}

// Tune performs one tuning iteration
func (at *AutoTuner) Tune(ctx context.Context) (*OptimizationResult, error) {
	at.mu.Lock()
	defer at.mu.Unlock()
	
	// Collect current metrics
	currentMetrics := at.collectMetrics()
	currentScore := at.calculateScore(currentMetrics)
	
	// Try parameter adjustments
	bestImprovement := 0.0
	bestParams := make(map[string]float64)
	
	for name, param := range at.parameters {
		// Try increasing
		originalValue := param.CurrentValue
		param.CurrentValue = at.adjustParameter(param, 1.0)
		
		// Simulate or measure performance
		testMetrics := at.collectMetrics()
		testScore := at.calculateScore(testMetrics)
		improvement := testScore - currentScore
		
		if improvement > bestImprovement {
			bestImprovement = improvement
			bestParams[name] = param.CurrentValue
		}
		
		// Try decreasing
		param.CurrentValue = at.adjustParameter(param, -1.0)
		testMetrics = at.collectMetrics()
		testScore = at.calculateScore(testMetrics)
		improvement = testScore - currentScore
		
		if improvement > bestImprovement {
			bestImprovement = improvement
			bestParams[name] = param.CurrentValue
		}
		
		// Restore original value
		param.CurrentValue = originalValue
	}
	
	// Apply best parameters
	if bestImprovement > 0 {
		for name, value := range bestParams {
			at.parameters[name].CurrentValue = value
			at.parameters[name].History = append(at.parameters[name].History, value)
			
			if value > at.parameters[name].BestValue {
				at.parameters[name].BestValue = value
				at.parameters[name].BestScore = currentScore + bestImprovement
			}
		}
		at.stats.Improvements++
	} else if bestImprovement < 0 {
		at.stats.Degradations++
	}
	
	// Record result
	result := OptimizationResult{
		Iteration:   int(at.stats.TotalIterations),
		Parameters:  at.getCurrentParameters(),
		Metrics:     *currentMetrics,
		Score:       currentScore + bestImprovement,
		Improvement: bestImprovement,
		Timestamp:   time.Now(),
	}
	
	at.history = append(at.history, result)
	at.stats.TotalIterations++
	at.stats.LastTuningTime = time.Now()
	
	if result.Score > at.stats.BestScore {
		at.stats.BestScore = result.Score
	}
	
	// Update average improvement
	at.stats.AvgImprovement = (at.stats.AvgImprovement*float64(at.stats.TotalIterations-1) + bestImprovement) / float64(at.stats.TotalIterations)
	
	return &result, nil
}

// adjustParameter adjusts a parameter value
func (at *AutoTuner) adjustParameter(param *TunableParameter, direction float64) float64 {
	// Use gradient descent with momentum
	adjustment := direction * param.StepSize * at.learningRate
	
	// Add exploration noise
	if at.shouldExplore() {
		noise := (math.Sin(float64(time.Now().UnixNano())) * 0.5) * param.StepSize
		adjustment += noise
	}
	
	newValue := param.CurrentValue + adjustment
	
	// Clamp to bounds
	if newValue < param.MinValue {
		newValue = param.MinValue
	}
	if newValue > param.MaxValue {
		newValue = param.MaxValue
	}
	
	return newValue
}

// shouldExplore determines if exploration should occur
func (at *AutoTuner) shouldExplore() bool {
	// Simple random exploration
	return math.Sin(float64(time.Now().UnixNano())) > (1.0 - at.config.ExplorationRate)
}

// collectMetrics collects current performance metrics
func (at *AutoTuner) collectMetrics() *PerformanceMetrics {
	// In production, this would collect real metrics
	// For now, return simulated metrics
	return &PerformanceMetrics{
		Throughput:   1000.0,
		Latency:      50.0,
		HitRate:      0.85,
		CPUUsage:     0.60,
		MemoryUsage:  0.70,
		NetworkIO:    100.0,
		Timestamp:    time.Now(),
	}
}

// calculateScore calculates overall performance score
func (at *AutoTuner) calculateScore(metrics *PerformanceMetrics) float64 {
	// Weighted score calculation
	// Higher is better
	
	// Normalize metrics (0-1 scale)
	throughputScore := math.Min(metrics.Throughput/10000.0, 1.0)
	latencyScore := 1.0 - math.Min(metrics.Latency/1000.0, 1.0)
	hitRateScore := metrics.HitRate
	cpuScore := 1.0 - metrics.CPUUsage
	memoryScore := 1.0 - metrics.MemoryUsage
	
	// Weighted combination
	score := throughputScore*0.3 +
		latencyScore*0.3 +
		hitRateScore*0.2 +
		cpuScore*0.1 +
		memoryScore*0.1
	
	return score
}

// getCurrentParameters returns current parameter values
func (at *AutoTuner) getCurrentParameters() map[string]float64 {
	params := make(map[string]float64)
	for name, param := range at.parameters {
		params[name] = param.CurrentValue
	}
	return params
}

// GetParameter returns a parameter value
func (at *AutoTuner) GetParameter(name string) (float64, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()
	
	param, exists := at.parameters[name]
	if !exists {
		return 0, false
	}
	
	return param.CurrentValue, true
}

// GetOptimizationHistory returns optimization history
func (at *AutoTuner) GetOptimizationHistory() []OptimizationResult {
	at.mu.RLock()
	defer at.mu.RUnlock()
	
	history := make([]OptimizationResult, len(at.history))
	copy(history, at.history)
	return history
}

// GetStats returns auto-tuning statistics
func (at *AutoTuner) GetStats() AutoTuningStats {
	at.mu.RLock()
	defer at.mu.RUnlock()
	
	return at.stats
}

// CostOptimizer optimizes for cost efficiency
type CostOptimizer struct {
	config CostOptimizerConfig
	mu     sync.RWMutex
	
	// Cost tracking
	costs map[string]*CostMetric
	
	// Optimization targets
	targets map[string]float64
	
	// Statistics
	stats CostOptimizationStats
}

// CostOptimizerConfig configures cost optimization
type CostOptimizerConfig struct {
	OptimizationInterval time.Duration
	CostThreshold        float64
	EnableAutoScaling    bool
	EnableCostAlerts     bool
}

// CostMetric tracks cost for a resource
type CostMetric struct {
	Resource     string
	Usage        float64
	Cost         float64
	CostPerUnit  float64
	Timestamp    time.Time
	Trend        float64
}

// CostOptimizationStats contains cost optimization statistics
type CostOptimizationStats struct {
	TotalCost         float64
	CostSavings       float64
	OptimizationsMade int64
	AvgCostReduction  float64
}

// NewCostOptimizer creates a new cost optimizer
func NewCostOptimizer(config CostOptimizerConfig) *CostOptimizer {
	if config.OptimizationInterval == 0 {
		config.OptimizationInterval = 1 * time.Hour
	}
	if config.CostThreshold == 0 {
		config.CostThreshold = 1000.0
	}
	
	return &CostOptimizer{
		config:  config,
		costs:   make(map[string]*CostMetric),
		targets: make(map[string]float64),
	}
}

// RecordCost records a cost metric
func (co *CostOptimizer) RecordCost(resource string, usage, costPerUnit float64) {
	co.mu.Lock()
	defer co.mu.Unlock()
	
	cost := usage * costPerUnit
	
	metric := &CostMetric{
		Resource:    resource,
		Usage:       usage,
		Cost:        cost,
		CostPerUnit: costPerUnit,
		Timestamp:   time.Now(),
	}
	
	// Calculate trend
	if existing, exists := co.costs[resource]; exists {
		metric.Trend = (cost - existing.Cost) / existing.Cost
	}
	
	co.costs[resource] = metric
	co.stats.TotalCost += cost
}

// OptimizeCosts optimizes costs
func (co *CostOptimizer) OptimizeCosts(ctx context.Context) (*CostOptimizationResult, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	
	totalSavings := 0.0
	recommendations := make([]CostRecommendation, 0)
	
	for resource, metric := range co.costs {
		// Check if cost exceeds threshold
		if metric.Cost > co.config.CostThreshold {
			// Generate recommendation
			rec := CostRecommendation{
				Resource:    resource,
				CurrentCost: metric.Cost,
				Action:      "reduce_usage",
				Savings:     metric.Cost * 0.2, // 20% reduction
				Priority:    "high",
			}
			recommendations = append(recommendations, rec)
			totalSavings += rec.Savings
		}
		
		// Check trend
		if metric.Trend > 0.1 { // 10% increase
			rec := CostRecommendation{
				Resource:    resource,
				CurrentCost: metric.Cost,
				Action:      "optimize_usage",
				Savings:     metric.Cost * 0.1,
				Priority:    "medium",
			}
			recommendations = append(recommendations, rec)
			totalSavings += rec.Savings
		}
	}
	
	co.stats.CostSavings += totalSavings
	co.stats.OptimizationsMade++
	
	return &CostOptimizationResult{
		TotalSavings:    totalSavings,
		Recommendations: recommendations,
		Timestamp:       time.Now(),
	}, nil
}

// CostOptimizationResult represents cost optimization results
type CostOptimizationResult struct {
	TotalSavings    float64
	Recommendations []CostRecommendation
	Timestamp       time.Time
}

// CostRecommendation represents a cost optimization recommendation
type CostRecommendation struct {
	Resource    string
	CurrentCost float64
	Action      string
	Savings     float64
	Priority    string
}

// GetStats returns cost optimization statistics
func (co *CostOptimizer) GetStats() CostOptimizationStats {
	co.mu.RLock()
	defer co.mu.RUnlock()
	
	return co.stats
}