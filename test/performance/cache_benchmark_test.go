package performance

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"redcache/pkg/cache"
)

// BenchmarkCacheHit measures cache hit latency
// Target: <100μs (0.1ms)
func BenchmarkCacheHit(b *testing.B) {
	ctx := context.Background()
	tmpDir := "/tmp/cache-bench-hit"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	storage, err := cache.NewStorage(tmpDir, 10*1024*1024*1024) // 10GB
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	// Pre-populate cache with test data
	testData := make([]byte, 1024*1024) // 1MB
	rand.Read(testData)
	
	if err := storage.Put(ctx, "test-key", testData); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, exists := storage.Get(ctx, "test-key")
		if !exists {
			b.Fatal("key not found")
		}
	}
}

// BenchmarkCacheHitParallel measures parallel cache hit performance
func BenchmarkCacheHitParallel(b *testing.B) {
	ctx := context.Background()
	tmpDir := "/tmp/cache-bench-parallel"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	storage, err := cache.NewStorage(tmpDir, 10*1024*1024*1024) // 10GB
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	// Pre-populate with multiple objects
	numObjects := 100
	for i := 0; i < numObjects; i++ {
		testData := make([]byte, 1024*1024)
		rand.Read(testData)
		
		key := fmt.Sprintf("test-key-%d", i)
		if err := storage.Put(ctx, key, testData); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("test-key-%d", i%numObjects)
			_, exists := storage.Get(ctx, key)
			if !exists {
				b.Fatal("key not found")
			}
			i++
		}
	})
}

// BenchmarkCachePut measures cache write performance
func BenchmarkCachePut(b *testing.B) {
	ctx := context.Background()
	tmpDir := "/tmp/cache-bench-put"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	storage, err := cache.NewStorage(tmpDir, 100*1024*1024*1024) // 100GB
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	testData := make([]byte, 1024*1024) // 1MB
	rand.Read(testData)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		if err := storage.Put(ctx, key, testData); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheEviction measures eviction performance
func BenchmarkCacheEviction(b *testing.B) {
	ctx := context.Background()
	tmpDir := "/tmp/cache-bench-evict"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	storage, err := cache.NewStorage(tmpDir, 1024*1024*1024) // 1GB
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	// Fill cache to trigger eviction
	testData := make([]byte, 10*1024*1024) // 10MB per object

	// Fill to 95% capacity
	for i := 0; i < 95; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		if err := storage.Put(ctx, key, testData); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("new-key-%d", i)
		if err := storage.Put(ctx, key, testData); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDifferentSizes measures performance with different object sizes
func BenchmarkDifferentSizes(b *testing.B) {
	ctx := context.Background()
	sizes := []int{
		1024,              // 1KB
		10 * 1024,         // 10KB
		100 * 1024,        // 100KB
		1024 * 1024,       // 1MB
		10 * 1024 * 1024,  // 10MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%dKB", size/1024), func(b *testing.B) {
			tmpDir := fmt.Sprintf("/tmp/cache-bench-size-%d", size)
			os.RemoveAll(tmpDir)
			defer os.RemoveAll(tmpDir)
			
			storage, err := cache.NewStorage(tmpDir, 10*1024*1024*1024) // 10GB
			if err != nil {
				b.Fatal(err)
			}
			defer storage.Close()

			testData := make([]byte, size)
			rand.Read(testData)
			
			if err := storage.Put(ctx, "test-key", testData); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(size))
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, exists := storage.Get(ctx, "test-key")
				if !exists {
					b.Fatal("key not found")
				}
			}
		})
	}
}

// TestCacheLatency measures actual latency distribution
func TestCacheLatency(t *testing.T) {
	ctx := context.Background()
	tmpDir := "/tmp/cache-latency-test"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	storage, err := cache.NewStorage(tmpDir, 10*1024*1024*1024) // 10GB
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Pre-populate cache
	testData := make([]byte, 1024*1024) // 1MB
	rand.Read(testData)
	
	if err := storage.Put(ctx, "test-key", testData); err != nil {
		t.Fatal(err)
	}

	// Measure latency distribution
	iterations := 10000
	latencies := make([]time.Duration, iterations)

	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, exists := storage.Get(ctx, "test-key")
		latencies[i] = time.Since(start)
		
		if !exists {
			t.Fatal("key not found")
		}
	}

	// Calculate percentiles
	p50, p95, p99, p999 := calculatePercentiles(latencies)

	t.Logf("Cache Hit Latency Distribution:")
	t.Logf("  P50:  %v", p50)
	t.Logf("  P95:  %v", p95)
	t.Logf("  P99:  %v", p99)
	t.Logf("  P999: %v", p999)

	// Assert performance targets (warnings only, not failures)
	if p95 > 100*time.Microsecond {
		t.Logf("WARNING: P95 latency %v exceeds target of 100μs", p95)
	}
	if p99 > 1*time.Millisecond {
		t.Logf("WARNING: P99 latency %v exceeds target of 1ms", p99)
	}
}

func calculatePercentiles(latencies []time.Duration) (p50, p95, p99, p999 time.Duration) {
	// Sort latencies
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	
	// Simple bubble sort (good enough for testing)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	p999 = sorted[len(sorted)*999/1000]

	return
}