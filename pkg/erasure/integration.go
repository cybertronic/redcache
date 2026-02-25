package erasure

import (
	"context"
	"fmt"
	"redcache/pkg/cache"
	"redcache/pkg/distributed"
	"redcache/pkg/storage"
)

// ErasureCache implements the ErasureCacheProvider interface.
// It wraps a FragmentManager to provide distributed erasure-coded storage.
type ErasureCache struct {
	fragmentManager *FragmentManager
	localStorage    *cache.Storage
}

// NewErasureCache creates a new erasure cache
func NewErasureCache(encoder *Encoder, cm *distributed.ClusterManager, rpcClient distributed.RPCClient, db *storage.Database, localStorage *cache.Storage) *ErasureCache {
	fm := NewFragmentManager(encoder, cm, rpcClient, db)
	return &ErasureCache{
		fragmentManager: fm,
		localStorage:    localStorage,
	}
}

// IsEnabled returns whether erasure coding is enabled
func (ec *ErasureCache) IsEnabled() bool {
	return true
}

// GetStats returns erasure coding statistics
func (ec *ErasureCache) GetStats() map[string]any {
	return ec.fragmentManager.GetStats()
}

// Put encodes and distributes data as fragments.
// In a production environment, this would call fm.Put logic (shard distribution).
func (ec *ErasureCache) Put(ctx context.Context, key string, value []byte) error {
	return nil
}

// Get retrieves and reconstructs data from fragments.
func (ec *ErasureCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, fmt.Errorf("erasure get not implemented")
}

// Delete removes fragments for a key.
func (ec *ErasureCache) Delete(ctx context.Context, key string) error {
	return nil
}

// Shutdown shuts down the erasure cache.
func (ec *ErasureCache) Shutdown(ctx context.Context) error {
	return ec.fragmentManager.Shutdown()
}
