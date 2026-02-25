package distributed

import (
	"context"
	"time"
)

// RPCClient defines the interface for RPC client operations used by
// erasure coding and rebalancer. Signatures match GRPCClient methods.
type RPCClient interface {
	// SetValue sets a value on a remote node
	SetValue(ctx context.Context, addr string, port int, key string, value []byte, ttl time.Duration) error

	// GetValue gets a value from a remote node
	GetValue(ctx context.Context, addr string, port int, key string) ([]byte, error)

	// DeleteValue deletes a value from a remote node
	DeleteValue(ctx context.Context, addr string, port int, key string) error
}

// Ensure GRPCClient implements the RPCClient interface
var _ RPCClient = (*GRPCClient)(nil)