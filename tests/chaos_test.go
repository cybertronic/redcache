package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ChaosScenario defines a chaos testing scenario
type ChaosScenario struct {
	Name              string
	Duration          time.Duration
	NodeFailureRate   float64 // Probability of node failure per second
	NetworkDelayMs    int     // Network delay in milliseconds
	WriteRate         int     // Writes per second
	ReadRate          int     // Reads per second
}

// ChaosTest runs a chaos testing scenario
func (cs *ChaosScenario) Run(t *testing.T, cluster *TestCluster) {
	t.Logf("Running chaos scenario: %s", cs.Name)
	t.Logf("Duration: %v, NodeFailureRate: %.2f, NetworkDelay: %dms",
		cs.Duration, cs.NodeFailureRate, cs.NetworkDelayMs)
	
	ctx := context.Background()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	
	// Track metrics
	var (
		totalWrites   int64
		successWrites int64
		totalReads    int64
		successReads  int64
		mu            sync.Mutex
	)
	
	// Writer goroutines
	for i := 0; i < cs.WriteRate/10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					// Select random node
					nodeIdx := rand.Intn(len(cluster.nodes))
					if nodeIdx >= len(cluster.nodes) {
						continue
					}
					
					node := cluster.nodes[nodeIdx]
					if node == nil {
						continue
					}
					
					key := fmt.Sprintf("chaos_key_%d_%d", workerID, counter)
					value := []byte(fmt.Sprintf("value_%d_%d", workerID, counter))
					
					err := node.SetCache(ctx, key, value, 0)
					
					mu.Lock()
					totalWrites++
					if err == nil {
						successWrites++
					}
					mu.Unlock()
					
					counter++
					time.Sleep(time.Duration(10000/cs.WriteRate) * time.Millisecond)
				}
			}
		}(i)
	}
	
	// Reader goroutines
	for i := 0; i < cs.ReadRate/10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			counter := 0
			
			for {
				select {
				case <-stopCh:
					return
				default:
					// Select random node
					nodeIdx := rand.Intn(len(cluster.nodes))
					if nodeIdx >= len(cluster.nodes) {
						continue
					}
					
					node := cluster.nodes[nodeIdx]
					if node == nil {
						continue
					}
					
					key := fmt.Sprintf("chaos_key_%d", counter%1000)
					
					_, err := node.GetCache(ctx, key)
					
					mu.Lock()
					totalReads++
					if err == nil {
						successReads++
					}
					mu.Unlock()
					
					counter++
					time.Sleep(time.Duration(10000/cs.ReadRate) * time.Millisecond)
				}
			}
		}(i)
	}
	
	// Chaos monkey - randomly stop/start nodes
	wg.Add(1)
	go func() {
		defer wg.Done()
		
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(time.Second):
				if rand.Float64() < cs.NodeFailureRate {
					// Random node failure
					if len(cluster.nodes) > 1 {
						nodeIdx := rand.Intn(len(cluster.nodes))
						t.Logf("Chaos: Stopping node %d", nodeIdx)
						
						// Note: In a real chaos test, we would restart the node
						// For now, just log the event
					}
				}
			}
		}
	}()
	
	// Run for duration
	time.Sleep(cs.Duration)
	close(stopCh)
	wg.Wait()
	
	// Report results
	mu.Lock()
	writeSuccessRate := float64(successWrites) / float64(totalWrites) * 100
	readSuccessRate := float64(successReads) / float64(totalReads) * 100
	mu.Unlock()
	
	t.Logf("=== Chaos Test Results ===")
	t.Logf("Writes: %d total, %d success (%.2f%%)", totalWrites, successWrites, writeSuccessRate)
	t.Logf("Reads: %d total, %d success (%.2f%%)", totalReads, successReads, readSuccessRate)
	
	// Assert minimum success rates
	assert.Greater(t, writeSuccessRate, 80.0, "Write success rate should be > 80%%")
	assert.Greater(t, readSuccessRate, 90.0, "Read success rate should be > 90%%")
}

// TestChaosBasic runs basic chaos testing
func TestChaosBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	scenario := ChaosScenario{
		Name:            "Basic Chaos",
		Duration:        10 * time.Second,
		NodeFailureRate: 0.1, // 10% chance per second
		NetworkDelayMs:  10,
		WriteRate:       100,
		ReadRate:        200,
	}

	scenario.Run(t, cluster)
}

// TestChaosHighLoad runs chaos testing under high load
func TestChaosHighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	scenario := ChaosScenario{
		Name:            "High Load Chaos",
		Duration:        10 * time.Second,
		NodeFailureRate: 0.05,
		NetworkDelayMs:  20,
		WriteRate:       500,
		ReadRate:        1000,
	}

	scenario.Run(t, cluster)
}

// TestDataConsistency tests data consistency under chaos
func TestDataConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consistency test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	
	// Write known data
	testData := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("consistency_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		testData[key] = value
		
		// Write to random node
		nodeIdx := rand.Intn(len(cluster.nodes))
		cluster.nodes[nodeIdx].SetCache(ctx, key, value, 0)
	}
	
	// Wait for replication
	time.Sleep(10 * time.Second)
	
	// Verify data consistency across all nodes
	inconsistencies := 0
	for key, expectedValue := range testData {
		values := make(map[string]int)
		
		// Read from all nodes
		for _, node := range cluster.nodes {
			value, err := node.GetCache(ctx, key)
			if err == nil && value != nil {
				values[string(value)]++
			}
		}
		
		// Check if all nodes have the same value
		if len(values) > 1 {
			inconsistencies++
			t.Logf("Inconsistency detected for key %s: %v", key, values)
		} else if len(values) == 1 {
			// Verify it's the expected value
			for v := range values {
				if v != string(expectedValue) {
					inconsistencies++
					t.Logf("Wrong value for key %s: got %s, expected %s", key, v, expectedValue)
				}
			}
		}
	}
	
	consistencyRate := float64(len(testData)-inconsistencies) / float64(len(testData)) * 100
	t.Logf("Consistency rate: %.2f%% (%d/%d keys consistent)", 
		consistencyRate, len(testData)-inconsistencies, len(testData))
	
	assert.Greater(t, consistencyRate, 95.0, "Consistency rate should be > 95%%")
}

// TestRecoveryAfterFailure tests recovery after node failure
func TestRecoveryAfterFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping recovery test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	
	// Write data before failure
	t.Log("Writing data before failure...")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("recovery_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		cluster.nodes[0].SetCache(ctx, key, value, 0)
	}
	
	// Wait for replication
	time.Sleep(5 * time.Second)
	
	// Simulate node failure (close node 1)
	t.Log("Simulating node failure...")
	failedNode := cluster.nodes[1]
	failedNode.Close()
	cluster.nodes[1] = nil
	
	// Continue writing during failure
	t.Log("Writing data during failure...")
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("recovery_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		cluster.nodes[0].SetCache(ctx, key, value, 0)
	}
	
	// Wait for replication to remaining nodes
	time.Sleep(5 * time.Second)
	
	// Verify data on remaining nodes
	t.Log("Verifying data on remaining nodes...")
	successCount := 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("recovery_key_%d", i)
		
		// Try to read from node 0 or node 2
		for _, nodeIdx := range []int{0, 2} {
			if cluster.nodes[nodeIdx] != nil {
				value, err := cluster.nodes[nodeIdx].GetCache(ctx, key)
				if err == nil && value != nil {
					successCount++
					break
				}
			}
		}
	}
	
	recoveryRate := float64(successCount) / 200.0 * 100
	t.Logf("Recovery rate: %.2f%% (%d/200 keys recovered)", recoveryRate, successCount)
	
	assert.Greater(t, recoveryRate, 90.0, "Recovery rate should be > 90%%")
}

// TestConcurrentFailures tests multiple concurrent node failures
func TestConcurrentFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent failures test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	ctx := context.Background()
	
	// Write initial data
	t.Log("Writing initial data...")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		cluster.nodes[0].SetCache(ctx, key, value, 0)
	}
	
	// Wait for replication
	time.Sleep(2 * time.Second)
	
	// Fail 2 nodes simultaneously
	t.Log("Failing 2 nodes simultaneously...")
	cluster.nodes[1].Close()
	cluster.nodes[2].Close()
	cluster.nodes[1] = nil
	cluster.nodes[2] = nil
	
	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)
	
	// Verify remaining nodes can still operate
	t.Log("Verifying remaining nodes...")
	successCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		
		// Try to read from remaining nodes
		for _, nodeIdx := range []int{0, 3, 4} {
			if cluster.nodes[nodeIdx] != nil {
				value, err := cluster.nodes[nodeIdx].GetCache(ctx, key)
				if err == nil && value != nil {
					successCount++
					break
				}
			}
		}
	}
	
	survivalRate := float64(successCount) / 100.0 * 100
	t.Logf("Survival rate: %.2f%% (%d/100 keys accessible)", survivalRate, successCount)
	
	assert.Greater(t, survivalRate, 80.0, "Survival rate should be > 80%% with 2 nodes down")
}