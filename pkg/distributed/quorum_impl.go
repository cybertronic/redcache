package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QuorumSet performs a quorum-based set operation
func (qm *QuorumManager) QuorumSet(ctx context.Context, key string, value []byte) error {
	var lastErr error
	
	for retry := 0; retry <= qm.config.RetryCount; retry++ {
		if retry > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(qm.config.RetryDelay):
				// Retry after delay
			}
		}
		
		err := qm.quorumSetOnce(ctx, key, value)
		if err == nil {
			return nil
		}
		
		lastErr = err
	}
	
	return fmt.Errorf("quorum set failed after %d retries: %w", qm.config.RetryCount, lastErr)
}

// quorumSetOnce performs a single quorum set attempt
func (qm *QuorumManager) quorumSetOnce(ctx context.Context, key string, value []byte) error {
	// Get all live nodes
	nodes := qm.cm.GetAliveNodes()
	if len(nodes) < qm.config.QuorumSize {
		return fmt.Errorf("insufficient nodes for quorum: have %d, need %d",
			len(nodes), qm.config.QuorumSize)
	}
	
	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, qm.config.Timeout)
	defer cancel()
	
	// Send write requests to all nodes
	results := make(chan error, len(nodes))
	var wg sync.WaitGroup
	
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			
			// Send write request with timeout
			err := qm.rpcClient.SetValue(timeoutCtx, addr, qm.config.RPCPort, key, value, 0)
			results <- err
		}(node.Address)
	}
	
	// Wait for all requests to complete
	go func() {
		wg.Wait()
		close(results)
	}()
	
	// Check if we have quorum
	successCount := 0
	failureCount := 0
	
	for err := range results {
		if err == nil {
			successCount++
			if successCount >= qm.config.WriteQuorum {
				return nil // Quorum achieved
			}
		} else {
			failureCount++
			if failureCount > len(nodes)-qm.config.WriteQuorum {
				// Too many failures to achieve quorum
				return fmt.Errorf("quorum not reachable: %d failures, need %d successes",
					failureCount, qm.config.WriteQuorum)
			}
		}
	}
	
	if successCount >= qm.config.WriteQuorum {
		return nil
	}
	
	return fmt.Errorf("quorum not reached: %d/%d successful, need %d",
		successCount, len(nodes), qm.config.WriteQuorum)
}

// QuorumDelete performs a quorum-based delete operation
func (qm *QuorumManager) QuorumDelete(ctx context.Context, key string) error {
	var lastErr error
	
	for retry := 0; retry <= qm.config.RetryCount; retry++ {
		if retry > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(qm.config.RetryDelay):
				// Retry after delay
			}
		}
		
		err := qm.quorumDeleteOnce(ctx, key)
		if err == nil {
			return nil
		}
		
		lastErr = err
	}
	
	return fmt.Errorf("quorum delete failed after %d retries: %w", qm.config.RetryCount, lastErr)
}

// quorumDeleteOnce performs a single quorum delete attempt
func (qm *QuorumManager) quorumDeleteOnce(ctx context.Context, key string) error {
	// Get all live nodes
	nodes := qm.cm.GetAliveNodes()
	if len(nodes) < qm.config.QuorumSize {
		return fmt.Errorf("insufficient nodes for quorum: have %d, need %d",
			len(nodes), qm.config.QuorumSize)
	}
	
	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, qm.config.Timeout)
	defer cancel()
	
	// Send delete requests to all nodes
	results := make(chan error, len(nodes))
	var wg sync.WaitGroup
	
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			
			// Send delete request with timeout
			err := qm.rpcClient.DeleteValue(timeoutCtx, addr, qm.config.RPCPort, key)
			results <- err
		}(node.Address)
	}
	
	// Wait for all requests to complete
	go func() {
		wg.Wait()
		close(results)
	}()
	
	// Check if we have quorum
	successCount := 0
	failureCount := 0
	
	for err := range results {
		if err == nil {
			successCount++
			if successCount >= qm.config.WriteQuorum {
				return nil // Quorum achieved
			}
		} else {
			failureCount++
			if failureCount > len(nodes)-qm.config.WriteQuorum {
				// Too many failures to achieve quorum
				return fmt.Errorf("quorum not reachable: %d failures, need %d successes",
					failureCount, qm.config.WriteQuorum)
			}
		}
	}
	
	if successCount >= qm.config.WriteQuorum {
		return nil
	}
	
	return fmt.Errorf("quorum not reached: %d/%d successful, need %d",
		successCount, len(nodes), qm.config.WriteQuorum)
}

// QuorumGet performs a quorum-based get operation (optional enhancement)
func (qm *QuorumManager) QuorumGet(ctx context.Context, key string) ([]byte, error) {
	// For reads, we can use a simpler approach
	// Try to read from one node, if that fails, try another
	
	nodes := qm.cm.GetAliveNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no live nodes available")
	}
	
	var lastErr error
	
	for _, node := range nodes {
		value, err := qm.rpcClient.GetValue(ctx, node.Address, qm.config.RPCPort, key)
		if err == nil {
			return value, nil
		}
		lastErr = err
	}
	
	return nil, fmt.Errorf("quorum get failed: %w", lastErr)
}