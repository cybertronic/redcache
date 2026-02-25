package monitoring

import (
	"context"
	"math"
	"sync"
	"time"
)

// AnalyticsEngine implements advanced metrics and analytics
type AnalyticsEngine struct {
	config AnalyticsConfig
	mu     sync.RWMutex
	
	// Time series data
	metrics     map[string]*MetricTimeSeries
	
	// Aggregations
	aggregations map[string]*Aggregation
	
	// Anomaly detection
	anomalyDetector *AnomalyDetector
	
	// Predictions
	predictor *MetricPredictor
	
	// Statistics
	stats AnalyticsStats
}

// AnalyticsConfig configures the analytics engine
type AnalyticsConfig struct {
	RetentionPeriod    time.Duration
	SamplingInterval   time.Duration
	AggregationWindow  time.Duration
	EnableAnomalyDetection bool
	EnablePrediction   bool
	AnomalyThreshold   float64
	PredictionHorizon  time.Duration
}

// MetricTimeSeries stores time series data for a metric
type MetricTimeSeries struct {
	Name       string
	DataPoints []DataPoint
	mu         sync.RWMutex
}

// DataPoint represents a single data point
type DataPoint struct {
	Timestamp time.Time
	Value     float64
	Tags      map[string]string
}

// Aggregation represents an aggregated metric
type Aggregation struct {
	Name      string
	Type      AggregationType
	Window    time.Duration
	Value     float64
	Count     int64
	Min       float64
	Max       float64
	Sum       float64
	Variance  float64
	UpdatedAt time.Time
}

// AggregationType defines aggregation types
type AggregationType int

const (
	AggregationAvg AggregationType = iota
	AggregationSum
	AggregationMin
	AggregationMax
	AggregationCount
	AggregationP50
	AggregationP95
	AggregationP99
)

// AnalyticsStats contains analytics statistics
type AnalyticsStats struct {
	TotalMetrics      int
	TotalDataPoints   int64
	AnomaliesDetected int64
	PredictionsMade   int64
	AvgProcessingTime time.Duration
}

// NewAnalyticsEngine creates a new analytics engine
func NewAnalyticsEngine(config AnalyticsConfig) *AnalyticsEngine {
	if config.RetentionPeriod == 0 {
		config.RetentionPeriod = 24 * time.Hour
	}
	if config.SamplingInterval == 0 {
		config.SamplingInterval = 10 * time.Second
	}
	if config.AggregationWindow == 0 {
		config.AggregationWindow = 1 * time.Minute
	}
	if config.AnomalyThreshold == 0 {
		config.AnomalyThreshold = 3.0 // 3 standard deviations
	}
	if config.PredictionHorizon == 0 {
		config.PredictionHorizon = 5 * time.Minute
	}
	
	ae := &AnalyticsEngine{
		config:       config,
		metrics:      make(map[string]*MetricTimeSeries),
		aggregations: make(map[string]*Aggregation),
	}
	
	if config.EnableAnomalyDetection {
		ae.anomalyDetector = NewAnomalyDetector(AnomalyDetectorConfig{
			Threshold: config.AnomalyThreshold,
		})
	}
	
	if config.EnablePrediction {
		ae.predictor = NewMetricPredictor(PredictorConfig{
			Horizon: config.PredictionHorizon,
		})
	}
	
	return ae
}

// RecordMetric records a metric value
func (ae *AnalyticsEngine) RecordMetric(ctx context.Context, name string, value float64, tags map[string]string) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	
	// Get or create time series
	ts, exists := ae.metrics[name]
	if !exists {
		ts = &MetricTimeSeries{
			Name:       name,
			DataPoints: make([]DataPoint, 0),
		}
		ae.metrics[name] = ts
		ae.stats.TotalMetrics++
	}
	
	// Add data point
	dp := DataPoint{
		Timestamp: time.Now(),
		Value:     value,
		Tags:      tags,
	}
	
	ts.mu.Lock()
	ts.DataPoints = append(ts.DataPoints, dp)
	ts.mu.Unlock()
	
	ae.stats.TotalDataPoints++
	
	// Update aggregations
	ae.updateAggregations(name, value)
	
	// Check for anomalies
	if ae.config.EnableAnomalyDetection {
		if ae.anomalyDetector.IsAnomaly(name, value) {
			ae.stats.AnomaliesDetected++
		}
	}
	
	// Clean old data
	ae.cleanOldData(ts)
}

// GetMetric retrieves metric data
func (ae *AnalyticsEngine) GetMetric(name string, start, end time.Time) []DataPoint {
	ae.mu.RLock()
	ts, exists := ae.metrics[name]
	ae.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	result := make([]DataPoint, 0)
	for _, dp := range ts.DataPoints {
		if dp.Timestamp.After(start) && dp.Timestamp.Before(end) {
			result = append(result, dp)
		}
	}
	
	return result
}

// GetAggregation retrieves an aggregation
func (ae *AnalyticsEngine) GetAggregation(name string, aggType AggregationType) *Aggregation {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	
	key := ae.aggregationKey(name, aggType)
	if agg, exists := ae.aggregations[key]; exists {
		aggCopy := *agg
		return &aggCopy
	}
	
	return nil
}

// PredictMetric predicts future metric values
func (ae *AnalyticsEngine) PredictMetric(name string, horizon time.Duration) []DataPoint {
	if !ae.config.EnablePrediction || ae.predictor == nil {
		return nil
	}
	
	ae.mu.RLock()
	ts, exists := ae.metrics[name]
	ae.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	ts.mu.RLock()
	dataPoints := make([]DataPoint, len(ts.DataPoints))
	copy(dataPoints, ts.DataPoints)
	ts.mu.RUnlock()
	
	predictions := ae.predictor.Predict(dataPoints, horizon)
	ae.stats.PredictionsMade++
	
	return predictions
}

// updateAggregations updates metric aggregations
func (ae *AnalyticsEngine) updateAggregations(name string, value float64) {
	aggregationTypes := []AggregationType{
		AggregationAvg,
		AggregationSum,
		AggregationMin,
		AggregationMax,
		AggregationCount,
	}
	
	for _, aggType := range aggregationTypes {
		key := ae.aggregationKey(name, aggType)
		
		agg, exists := ae.aggregations[key]
		if !exists {
			agg = &Aggregation{
				Name:   name,
				Type:   aggType,
				Window: ae.config.AggregationWindow,
				Min:    value,
				Max:    value,
			}
			ae.aggregations[key] = agg
		}
		
		// Update aggregation
		agg.Count++
		agg.Sum += value
		agg.UpdatedAt = time.Now()
		
		if value < agg.Min {
			agg.Min = value
		}
		if value > agg.Max {
			agg.Max = value
		}
		
		switch aggType {
		case AggregationAvg:
			agg.Value = agg.Sum / float64(agg.Count)
		case AggregationSum:
			agg.Value = agg.Sum
		case AggregationMin:
			agg.Value = agg.Min
		case AggregationMax:
			agg.Value = agg.Max
		case AggregationCount:
			agg.Value = float64(agg.Count)
		}
		
		// Update variance for standard deviation
		delta := value - agg.Value
		agg.Variance += delta * delta
	}
}

// cleanOldData removes data points older than retention period
func (ae *AnalyticsEngine) cleanOldData(ts *MetricTimeSeries) {
	cutoff := time.Now().Add(-ae.config.RetentionPeriod)
	
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	newDataPoints := make([]DataPoint, 0)
	for _, dp := range ts.DataPoints {
		if dp.Timestamp.After(cutoff) {
			newDataPoints = append(newDataPoints, dp)
		}
	}
	
	ts.DataPoints = newDataPoints
}

// aggregationKey generates a key for an aggregation
func (ae *AnalyticsEngine) aggregationKey(name string, aggType AggregationType) string {
	return name + ":" + aggType.String()
}

// GetStats returns analytics statistics
func (ae *AnalyticsEngine) GetStats() AnalyticsStats {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	
	return ae.stats
}

// String returns string representation of aggregation type
func (at AggregationType) String() string {
	switch at {
	case AggregationAvg:
		return "avg"
	case AggregationSum:
		return "sum"
	case AggregationMin:
		return "min"
	case AggregationMax:
		return "max"
	case AggregationCount:
		return "count"
	case AggregationP50:
		return "p50"
	case AggregationP95:
		return "p95"
	case AggregationP99:
		return "p99"
	default:
		return "unknown"
	}
}

// AnomalyDetector detects anomalies in metrics
type AnomalyDetector struct {
	config    AnomalyDetectorConfig
	mu        sync.RWMutex
	baselines map[string]*Baseline
}

// AnomalyDetectorConfig configures anomaly detection
type AnomalyDetectorConfig struct {
	Threshold      float64
	WindowSize     int
	LearningPeriod time.Duration
}

// Baseline represents a metric baseline
type Baseline struct {
	Mean     float64
	StdDev   float64
	Samples  []float64
	Updated  time.Time
}

// NewAnomalyDetector creates a new anomaly detector
func NewAnomalyDetector(config AnomalyDetectorConfig) *AnomalyDetector {
	if config.WindowSize == 0 {
		config.WindowSize = 100
	}
	if config.LearningPeriod == 0 {
		config.LearningPeriod = 1 * time.Hour
	}
	
	return &AnomalyDetector{
		config:    config,
		baselines: make(map[string]*Baseline),
	}
}

// IsAnomaly checks if a value is anomalous
func (ad *AnomalyDetector) IsAnomaly(metric string, value float64) bool {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	
	baseline, exists := ad.baselines[metric]
	if !exists {
		baseline = &Baseline{
			Samples: make([]float64, 0, ad.config.WindowSize),
		}
		ad.baselines[metric] = baseline
	}
	
	// Add sample
	baseline.Samples = append(baseline.Samples, value)
	if len(baseline.Samples) > ad.config.WindowSize {
		baseline.Samples = baseline.Samples[1:]
	}
	
	// Update baseline statistics
	ad.updateBaseline(baseline)
	
	// Check if anomalous (z-score method)
	if baseline.StdDev == 0 {
		return false
	}
	
	zScore := math.Abs((value - baseline.Mean) / baseline.StdDev)
	return zScore > ad.config.Threshold
}

// updateBaseline updates baseline statistics
func (ad *AnomalyDetector) updateBaseline(baseline *Baseline) {
	if len(baseline.Samples) == 0 {
		return
	}
	
	// Calculate mean
	sum := 0.0
	for _, v := range baseline.Samples {
		sum += v
	}
	baseline.Mean = sum / float64(len(baseline.Samples))
	
	// Calculate standard deviation
	variance := 0.0
	for _, v := range baseline.Samples {
		diff := v - baseline.Mean
		variance += diff * diff
	}
	variance /= float64(len(baseline.Samples))
	baseline.StdDev = math.Sqrt(variance)
	baseline.Updated = time.Now()
}

// MetricPredictor predicts future metric values
type MetricPredictor struct {
	config PredictorConfig
	mu     sync.RWMutex
	models map[string]*PredictionModel
}

// PredictorConfig configures metric prediction
type PredictorConfig struct {
	Horizon        time.Duration
	ModelType      string
	UpdateInterval time.Duration
}

// PredictionModel represents a prediction model
type PredictionModel struct {
	Coefficients []float64
	Intercept    float64
	LastUpdated  time.Time
	Accuracy     float64
}

// NewMetricPredictor creates a new metric predictor
func NewMetricPredictor(config PredictorConfig) *MetricPredictor {
	if config.UpdateInterval == 0 {
		config.UpdateInterval = 5 * time.Minute
	}
	
	return &MetricPredictor{
		config: config,
		models: make(map[string]*PredictionModel),
	}
}

// Predict predicts future values
func (mp *MetricPredictor) Predict(dataPoints []DataPoint, horizon time.Duration) []DataPoint {
	if len(dataPoints) < 10 {
		return nil
	}
	
	// Simple linear regression for prediction
	// In production, use more sophisticated models (ARIMA, LSTM, etc.)
	
	// Extract values and timestamps
	n := len(dataPoints)
	x := make([]float64, n)
	y := make([]float64, n)
	
	baseTime := dataPoints[0].Timestamp
	for i, dp := range dataPoints {
		x[i] = float64(dp.Timestamp.Sub(baseTime).Seconds())
		y[i] = dp.Value
	}
	
	// Calculate linear regression coefficients
	slope, intercept := mp.linearRegression(x, y)
	
	// Generate predictions
	lastTime := dataPoints[n-1].Timestamp
	interval := time.Duration(x[n-1]-x[n-2]) * time.Second
	
	predictions := make([]DataPoint, 0)
	steps := int(horizon / interval)
	
	for i := 1; i <= steps; i++ {
		predTime := lastTime.Add(time.Duration(i) * interval)
		predX := float64(predTime.Sub(baseTime).Seconds())
		predY := slope*predX + intercept
		
		predictions = append(predictions, DataPoint{
			Timestamp: predTime,
			Value:     predY,
		})
	}
	
	return predictions
}

// linearRegression performs simple linear regression
func (mp *MetricPredictor) linearRegression(x, y []float64) (slope, intercept float64) {
	n := float64(len(x))
	
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0
	
	for i := range x {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
	}
	
	slope = (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	intercept = (sumY - slope*sumX) / n
	
	return slope, intercept
}