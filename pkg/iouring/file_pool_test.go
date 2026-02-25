//go:build linux

package iouring

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPinnedBufferPool(t *testing.T) {
	pool, err := NewPinnedBufferPool(4, 1024)
	require.NoError(t, err)
	defer pool.Close()

	assert.Equal(t, 4, pool.Count())
	assert.Equal(t, 1024, pool.BufferSize())

	// Acquire all buffers
	bufs := make([]*PinnedBuffer, 4)
	for i := range bufs {
		bufs[i] = pool.Acquire()
		require.NotNil(t, bufs[i], "should get buffer %d", i)
		assert.Equal(t, i, bufs[i].Index())
		assert.Equal(t, 1024, bufs[i].Len())
	}

	// TryAcquire should fail when pool is empty
	extra := pool.TryAcquire()
	assert.Nil(t, extra, "pool should be empty")

	// Release one and re-acquire
	pool.Release(bufs[0])
	bufs[0] = nil

	got := pool.TryAcquire()
	require.NotNil(t, got)
	pool.Release(got)

	// Release all
	for _, b := range bufs {
		if b != nil {
			pool.Release(b)
		}
	}
}

func TestPinnedBufferPoolInvalidArgs(t *testing.T) {
	_, err := NewPinnedBufferPool(0, 1024)
	assert.Error(t, err)

	_, err = NewPinnedBufferPool(4, 0)
	assert.Error(t, err)
}

func TestFilePoolFallback(t *testing.T) {
	// Create a FilePool in fallback mode (disabled)
	fp := &FilePool{features: &KernelFeatures{}, enabled: false}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.bin")
	data := []byte("hello, world from fallback")

	ctx := context.Background()

	// Write via fallback
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	f.Close()

	// Read via fallback
	f, err = os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	dst := make([]byte, len(data))
	n, err := fp.ReadAt(ctx, f, dst, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, dst)
}

func TestFilePoolIOUring(t *testing.T) {
	features := DetectFeatures()
	if !features.Available {
		t.Skip("io_uring not available on this kernel")
	}

	fp, err := NewFilePool(64, 8, 64*1024) // 64KB buffers
	require.NoError(t, err)
	defer fp.Close()

	assert.True(t, fp.Enabled())

	tmpDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test WriteFile + ReadFile round-trip
	t.Run("WriteReadRoundTrip", func(t *testing.T) {
		path := filepath.Join(tmpDir, "roundtrip.bin")
		original := make([]byte, 4096)
		for i := range original {
			original[i] = byte(i % 256)
		}

		err := fp.WriteFile(ctx, path, original, 0644)
		require.NoError(t, err)

		got, err := fp.ReadFile(ctx, path)
		require.NoError(t, err)
		assert.Equal(t, original, got)
	})

	// Test ReadAt / WriteAt
	t.Run("ReadAtWriteAt", func(t *testing.T) {
		path := filepath.Join(tmpDir, "readat.bin")
		data := []byte("io_uring fixed buffer test data")

		f, err := os.Create(path)
		require.NoError(t, err)
		_, err = f.Write(data)
		require.NoError(t, err)
		f.Close()

		f, err = os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		dst := make([]byte, len(data))
		n, err := fp.ReadAt(ctx, f, dst, 0)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, dst)
	})

	// Test large file (multi-chunk)
	t.Run("LargeFile", func(t *testing.T) {
		path := filepath.Join(tmpDir, "large.bin")
		// 200KB — larger than a single 64KB buffer
		large := make([]byte, 200*1024)
		for i := range large {
			large[i] = byte(i % 251)
		}

		err := fp.WriteFile(ctx, path, large, 0644)
		require.NoError(t, err)

		got, err := fp.ReadFile(ctx, path)
		require.NoError(t, err)
		assert.Equal(t, large, got)
	})

	// Test empty file
	t.Run("EmptyFile", func(t *testing.T) {
		path := filepath.Join(tmpDir, "empty.bin")
		err := fp.WriteFile(ctx, path, []byte{}, 0644)
		require.NoError(t, err)

		got, err := fp.ReadFile(ctx, path)
		require.NoError(t, err)
		assert.Equal(t, []byte{}, got)
	})
}

func TestFilePoolMetrics(t *testing.T) {
	features := DetectFeatures()
	if !features.Available {
		t.Skip("io_uring not available on this kernel")
	}

	// Reset global metrics
	m := GlobalMetrics()
	initialReads := m.FileReadsTotal.Load()

	fp, err := NewFilePool(64, 4, 64*1024)
	require.NoError(t, err)
	defer fp.Close()

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metrics.bin")
	data := []byte("metrics test")

	ctx := context.Background()
	err = fp.WriteFile(ctx, path, data, 0644)
	require.NoError(t, err)

	_, err = fp.ReadFile(ctx, path)
	require.NoError(t, err)

	// Reads counter should have incremented
	assert.Greater(t, m.FileReadsTotal.Load(), initialReads)
}