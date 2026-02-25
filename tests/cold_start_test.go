package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"redcache/pkg/distributed"

	"github.com/stretchr/testify/assert"
)

// TestColdStartThunderingHerd simulates 100 GPU nodes requesting the same dataset
func TestColdStartThunderingHerd(t *testing.T) {
	// Skip if short testing
	if testing.Short() {
		t.Skip("Skipping cold start test in short mode")
	}

	// Track object store requests
	var objectStoreRequests int64
	var objectStoreMu sync.Mutex
	objectStoreData := make(map[string][]byte)

	// Mock object store fetch function
	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt64(&objectStoreRequests, 1)
		
		// Simulate network latency
		time.Sleep(100 * time.Millisecond)
		
		// Return mock data
		objectStoreMu.Lock()
		defer objectStoreMu.Unlock()
		
		if data, exists := objectStoreData[key]; exists {
			return data, nil
		}
		
		// Generate mock data (10MB)
		data := make([]byte, 10*1024*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}
		objectStoreData[key] = data
		
		return data, nil
	}

	// Create fetch coordinator with aggressive settings
	config := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: false, // Test per-node deduplication first
		RequestsPerSecond:            10,
		MaxConcurrentFetches:         3,
		FetchTimeout:                 60 * time.Second,
		MaxQueuedRequests:            1000,
		QueueTimeout:                 30 * time.Second,
		MaxRetries:                   2,
		RetryBackoff:                 100 * time.Millisecond,
		RetryMaxBackoff:              1 * time.Second,
	}

	coordinator := distributed.NewFetchCoordinator(config, nil, nil, fetchFunc)

	// Simulate 100 concurrent requests for the same key
	numRequests := 100
	key := "large-dataset"
	
	var wg sync.WaitGroup
	errors := make([]error, numRequests)
	results := make([][]byte, numRequests)
	
	startTime := time.Now()
	
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			
			data, err := coordinator.Fetch(ctx, key)
			errors[idx] = err
			results[idx] = data
		}(i)
	}
	
	wg.Wait()
	duration := time.Since(startTime)
	
	// Verify results
	t.Logf("Cold start test completed in %v", duration)
	t.Logf("Object store requests: %d", atomic.LoadInt64(&objectStoreRequests))
	
	// Check that all requests succeeded
	successCount := 0
	for i, err := range errors {
		if err == nil {
			successCount++
			assert.NotNil(t, results[i], "Result %d should not be nil", i)
			assert.Equal(t, 10*1024*1024, len(results[i]), "Result %d should be 10MB", i)
		} else {
			t.Logf("Request %d failed: %v", i, err)
		}
	}
	
	t.Logf("Successful requests: %d/%d", successCount, numRequests)
	
	// Verify deduplication worked
	// With singleflight, we should have only 1 object store request
	// (all 100 requests coalesced into 1)
	assert.LessOrEqual(t, atomic.LoadInt64(&objectStoreRequests), int64(3), 
		"Should have at most 3 object store requests (due to rate limiting)")
	
	// Get metrics
	metrics := coordinator.GetMetrics()
	t.Logf("Fetch Metrics:")
	t.Logf("  Coalesced Requests: %d", metrics.CoalescedRequests)
	t.Logf("  Object Store Fetches: %d", metrics.ObjectStoreFetches)
	t.Logf("  Rate Limited: %d", metrics.RateLimitedRequests)
	t.Logf("  Queued: %d", metrics.QueuedRequests)
	t.Logf("  Max Queue Depth: %d", metrics.MaxQueueDepth)
	t.Logf("  Total Wait Time: %v", metrics.TotalWaitTime)
	
	// Verify coalescing happened
	assert.Greater(t, metrics.CoalescedRequests, int64(90), 
		"Should have coalesced at least 90 requests")
}

// TestColdStartWithRateLimiting tests rate limiting behavior
func TestColdStartWithRateLimiting(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping rate limiting test in short mode")
	}

	var objectStoreRequests int64

	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt64(&objectStoreRequests, 1)
		return []byte("data"), nil
	}

	// Configure strict rate limiting: 5 requests per second
	config := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: false,
		RequestsPerSecond:            5,
		MaxConcurrentFetches:         10,
		FetchTimeout:                 10 * time.Second,
		MaxQueuedRequests:            100,
		QueueTimeout:                 10 * time.Second,
		MaxRetries:                   0,
		RetryBackoff:                 0,
		RetryMaxBackoff:              0,
	}

	coordinator := distributed.NewFetchCoordinator(config, nil, nil, fetchFunc)

	// Send 25 requests for different keys (should take ~5 seconds at 5 req/sec)
	numRequests := 25
	var wg sync.WaitGroup
	
	startTime := time.Now()
	
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			ctx := context.Background()
			key := fmt.Sprintf("key-%d", idx)
			_, _ = coordinator.Fetch(ctx, key)
		}(i)
	}
	
	wg.Wait()
	duration := time.Since(startTime)
	
	t.Logf("Rate limiting test completed in %v", duration)
	t.Logf("Object store requests: %d", atomic.LoadInt64(&objectStoreRequests))
	
	// Should take at least 4 seconds (25 requests / 5 per second = 5 seconds, minus some tolerance)
	assert.GreaterOrEqual(t, duration, 4*time.Second, 
		"Rate limiting should enforce ~5 requests per second")
	
	// Should have made 25 requests (one per unique key)
	assert.Equal(t, int64(numRequests), atomic.LoadInt64(&objectStoreRequests))
}

// TestColdStartConcurrencyLimit tests concurrent fetch limiting
func TestColdStartConcurrencyLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency limit test in short mode")
	}

	var activeFetches int64
	var maxActiveFetches int64
	var mu sync.Mutex

	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		// Track active fetches
		active := atomic.AddInt64(&activeFetches, 1)
		defer atomic.AddInt64(&activeFetches, -1)
		
		mu.Lock()
		if active > maxActiveFetches {
			maxActiveFetches = active
		}
		mu.Unlock()
		
		// Simulate slow fetch
		time.Sleep(500 * time.Millisecond)
		
		return []byte("data"), nil
	}

	// Configure max 3 concurrent fetches
	config := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: false,
		RequestsPerSecond:            100, // High rate limit
		MaxConcurrentFetches:         3,   // But only 3 concurrent
		FetchTimeout:                 10 * time.Second,
		MaxQueuedRequests:            100,
		QueueTimeout:                 10 * time.Second,
		MaxRetries:                   0,
		RetryBackoff:                 0,
		RetryMaxBackoff:              0,
	}

	coordinator := distributed.NewFetchCoordinator(config, nil, nil, fetchFunc)

	// Send 20 requests for different keys
	numRequests := 20
	var wg sync.WaitGroup
	
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			ctx := context.Background()
			key := fmt.Sprintf("key-%d", idx)
			_, _ = coordinator.Fetch(ctx, key)
		}(i)
	}
	
	wg.Wait()
	
	t.Logf("Max concurrent fetches: %d", maxActiveFetches)
	
	// Should never exceed 3 concurrent fetches
	assert.LessOrEqual(t, maxActiveFetches, int64(3), 
		"Should never exceed max concurrent fetches limit")
}

// TestColdStartMetrics tests that metrics are properly tracked
func TestColdStartMetrics(t *testing.T) {
	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		// Slow enough that concurrent requests for the same key will be coalesced
		time.Sleep(50 * time.Millisecond)
		return []byte("data"), nil
	}

	config := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: false,
		RequestsPerSecond:            10,
		MaxConcurrentFetches:         3,
		FetchTimeout:                 5 * time.Second,
		MaxQueuedRequests:            100,
		QueueTimeout:                 5 * time.Second,
		MaxRetries:                   0,
		RetryBackoff:                 0,
		RetryMaxBackoff:              0,
	}

	coordinator := distributed.NewFetchCoordinator(config, nil, nil, fetchFunc)

	// Make some requests
	ctx := context.Background()
	
	// Launch all 5 requests concurrently for the same key - they should be coalesced
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = coordinator.Fetch(ctx, "key1")
		}()
	}
	wg.Wait()
	
	// Get metrics
	metrics := coordinator.GetMetrics()
	
	t.Logf("Metrics:")
	t.Logf("  Coalesced: %d", metrics.CoalescedRequests)
	t.Logf("  Object Store Fetches: %d", metrics.ObjectStoreFetches)
	t.Logf("  Total Wait Time: %v", metrics.TotalWaitTime)
	
	// Verify metrics
	assert.Greater(t, metrics.CoalescedRequests, int64(0), "Should have coalesced requests")
	assert.Greater(t, metrics.ObjectStoreFetches, int64(0), "Should have fetched from object store")
}

// BenchmarkColdStart benchmarks cold start performance
func BenchmarkColdStart(b *testing.B) {
	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		// Simulate 10ms fetch
		time.Sleep(10 * time.Millisecond)
		return []byte("data"), nil
	}

	config := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: false,
		RequestsPerSecond:            100,
		MaxConcurrentFetches:         10,
		FetchTimeout:                 5 * time.Second,
		MaxQueuedRequests:            1000,
		QueueTimeout:                 5 * time.Second,
		MaxRetries:                   0,
		RetryBackoff:                 0,
		RetryMaxBackoff:              0,
	}

	coordinator := distributed.NewFetchCoordinator(config, nil, nil, fetchFunc)
	ctx := context.Background()

	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%10) // 10 unique keys
			_, _ = coordinator.Fetch(ctx, key)
			i++
		}
	})
}