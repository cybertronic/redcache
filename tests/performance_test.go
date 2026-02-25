package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"redcache/pkg/distributed"
)

// PerformanceMetrics tracks performance metrics
type PerformanceMetrics struct {
	TotalOps      atomic.Int64
	SuccessOps    atomic.Int64
	FailedOps     atomic.Int64
	TotalLatency  atomic.Int64 // in microseconds
	MinLatency    atomic.Int64
	MaxLatency    atomic.Int64
	StartTime     time.Time
	EndTime       time.Time
}

// RecordOp records an operation
func (pm *PerformanceMetrics) RecordOp(success bool, latency time.Duration) {
	pm.TotalOps.Add(1)
	if success {
		pm.SuccessOps.Add(1)
	} else {
		pm.FailedOps.Add(1)
	}
	
	latencyMicros := latency.Microseconds()
	pm.TotalLatency.Add(latencyMicros)
	
	// Update min latency
	for {
		current := pm.MinLatency.Load()
		if current == 0 || latencyMicros < current {
			if pm.MinLatency.CompareAndSwap(current, latencyMicros) {
				break
			}
		} else {
			break
		}
	}
	
	// Update max latency
	for {
		current := pm.MaxLatency.Load()
		if latencyMicros > current {
			if pm.MaxLatency.CompareAndSwap(current, latencyMicros) {
				break
			}
		} else {
			break
		}
	}
}

// Report prints performance report
func (pm *PerformanceMetrics) Report(t *testing.T) {
	duration := pm.EndTime.Sub(pm.StartTime)
	totalOps := pm.TotalOps.Load()
	successOps := pm.SuccessOps.Load()
	failedOps := pm.FailedOps.Load()
	
	throughput := float64(totalOps) / duration.Seconds()
	avgLatency := float64(pm.TotalLatency.Load()) / float64(totalOps)
	
	t.Logf("=== Performance Report ===")
	t.Logf("Duration: %v", duration)
	t.Logf("Total Operations: %d", totalOps)
	t.Logf("Successful: %d (%.2f%%)", successOps, float64(successOps)/float64(totalOps)*100)
	t.Logf("Failed: %d (%.2f%%)", failedOps, float64(failedOps)/float64(totalOps)*100)
	t.Logf("Throughput: %.2f ops/sec", throughput)
	t.Logf("Avg Latency: %.2f μs", avgLatency)
	t.Logf("Min Latency: %d μs", pm.MinLatency.Load())
	t.Logf("Max Latency: %d μs", pm.MaxLatency.Load())
}

// TestCacheWritePerformance tests cache write performance
func TestCacheWritePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	metrics := &PerformanceMetrics{}
	
	duration := 5 * time.Second
	concurrency := 10
	
	t.Logf("Running cache write performance test: %d concurrent workers for %v", concurrency, duration)
	
	metrics.StartTime = time.Now()
	
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	
	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			node := cluster.nodes[workerID%len(cluster.nodes)]
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("perf_key_%d_%d", workerID, counter)
					value := []byte(fmt.Sprintf("value_%d_%d", workerID, counter))
					
					start := time.Now()
					err := node.SetCache(ctx, key, value, 0)
					latency := time.Since(start)
					
					metrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	
	metrics.EndTime = time.Now()
	metrics.Report(t)
}

// TestCacheReadPerformance tests cache read performance
func TestCacheReadPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	
	// Prepare data
	t.Log("Preparing test data...")
	dataSize := 10000
	for i := 0; i < dataSize; i++ {
		key := fmt.Sprintf("read_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		cluster.nodes[0].SetCache(ctx, key, value, 0)
	}
	
	// Wait for replication
	time.Sleep(5 * time.Second)
	
	metrics := &PerformanceMetrics{}
	duration := 5 * time.Second
	concurrency := 20
	
	t.Logf("Running cache read performance test: %d concurrent workers for %v", concurrency, duration)
	
	metrics.StartTime = time.Now()
	
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	
	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			node := cluster.nodes[workerID%len(cluster.nodes)]
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("read_key_%d", counter%dataSize)
					
					start := time.Now()
					_, err := node.GetCache(ctx, key)
					latency := time.Since(start)
					
					metrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	
	metrics.EndTime = time.Now()
	metrics.Report(t)
}

// TestMixedWorkload tests mixed read/write workload
func TestMixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	
	readMetrics := &PerformanceMetrics{}
	writeMetrics := &PerformanceMetrics{}
	
	duration := 5 * time.Second
	readWorkers := 15
	writeWorkers := 5
	
	t.Logf("Running mixed workload test: %d readers, %d writers for %v", readWorkers, writeWorkers, duration)
	
	startTime := time.Now()
	readMetrics.StartTime = startTime
	writeMetrics.StartTime = startTime
	
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	
	// Start read workers
	for i := 0; i < readWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			node := cluster.nodes[workerID%len(cluster.nodes)]
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("mixed_key_%d", counter%1000)
					
					start := time.Now()
					_, err := node.GetCache(ctx, key)
					latency := time.Since(start)
					
					readMetrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Start write workers
	for i := 0; i < writeWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			node := cluster.nodes[workerID%len(cluster.nodes)]
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("mixed_key_%d", counter%1000)
					value := []byte(fmt.Sprintf("value_%d_%d", workerID, counter))
					
					start := time.Now()
					err := node.SetCache(ctx, key, value, 0)
					latency := time.Since(start)
					
					writeMetrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	
	endTime := time.Now()
	readMetrics.EndTime = endTime
	writeMetrics.EndTime = endTime
	
	t.Log("\n=== READ PERFORMANCE ===")
	readMetrics.Report(t)
	
	t.Log("\n=== WRITE PERFORMANCE ===")
	writeMetrics.Report(t)
}

// TestCoordinationPerformance tests coordination (Raft) performance
func TestCoordinationPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	leader := cluster.GetLeader()
	require.NotNil(t, leader)
	
	metrics := &PerformanceMetrics{}
	duration := 5 * time.Second
	concurrency := 5 // Lower concurrency for Raft
	
	t.Logf("Running coordination write performance test: %d concurrent workers for %v", concurrency, duration)
	
	metrics.StartTime = time.Now()
	
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	
	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("coord_key_%d_%d", workerID, counter)
					value := []byte(fmt.Sprintf("value_%d_%d", workerID, counter))
					
					start := time.Now()
					err := leader.SetCoordination(ctx, key, value)
					latency := time.Since(start)
					
					metrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	
	metrics.EndTime = time.Now()
	metrics.Report(t)
}

// TestLockPerformance tests distributed lock performance
func TestLockPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	leader := cluster.GetLeader()
	require.NotNil(t, leader)
	
	metrics := &PerformanceMetrics{}
	duration := 5 * time.Second
	concurrency := 10
	
	t.Logf("Running lock performance test: %d concurrent workers for %v", concurrency, duration)
	
	metrics.StartTime = time.Now()
	
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	
	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("lock_%d", workerID)
					
					start := time.Now()
					err := leader.AcquireLock(ctx, key, distributed.LockTypeExclusive)
					if err == nil {
						// Hold lock briefly
						time.Sleep(10 * time.Millisecond)
						leader.ReleaseLock(ctx, key)
					}
					latency := time.Since(start)
					
					metrics.RecordOp(err == nil, latency)
					counter++
				}
			}
		}(i)
	}
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	
	metrics.EndTime = time.Now()
	metrics.Report(t)
}

// TestScalability tests how performance scales with cluster size
func TestScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping scalability test in short mode")
	}

	clusterSizes := []int{1, 3, 5}
	duration := 5 * time.Second
	
	for _, size := range clusterSizes {
		t.Run(fmt.Sprintf("ClusterSize%d", size), func(t *testing.T) {
			cluster := NewTestCluster(t, size)
			cluster.Start(t)
			defer cluster.Stop()
			
			ctx := context.Background()
			metrics := &PerformanceMetrics{}
			
			metrics.StartTime = time.Now()
			
			// Single writer
			counter := 0
			stopTime := time.Now().Add(duration)
			
			for time.Now().Before(stopTime) {
				key := fmt.Sprintf("scale_key_%d", counter)
				value := []byte(fmt.Sprintf("value_%d", counter))
				
				start := time.Now()
				err := cluster.nodes[0].SetCache(ctx, key, value, 0)
				latency := time.Since(start)
				
				metrics.RecordOp(err == nil, latency)
				counter++
			}
			
			metrics.EndTime = time.Now()
			
			t.Logf("\n=== Cluster Size: %d ===", size)
			metrics.Report(t)
		})
	}
}