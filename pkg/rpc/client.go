package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "redcache/proto"
)

// Client provides RPC client functionality with connection pooling
type Client struct {
	mu          sync.RWMutex
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.CacheServiceClient
	
	// Configuration
	maxRetries      int
	retryDelay      time.Duration
	connectTimeout  time.Duration
	requestTimeout  time.Duration
	
	// Circuit breaker
	failures        map[string]int
	failureThreshold int
	resetTimeout    time.Duration
	
	// Metrics
	totalRequests   int64
	failedRequests  int64
}

// ClientConfig configures the RPC client
type ClientConfig struct {
	MaxRetries       int
	RetryDelay       time.Duration
	ConnectTimeout   time.Duration
	RequestTimeout   time.Duration
	FailureThreshold int
	ResetTimeout     time.Duration
}

// NewClient creates a new RPC client
func NewClient(config ClientConfig) *Client {
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 5
	}
	if config.ResetTimeout == 0 {
		config.ResetTimeout = 60 * time.Second
	}
	
	return &Client{
		connections:      make(map[string]*grpc.ClientConn),
		clients:          make(map[string]pb.CacheServiceClient),
		failures:         make(map[string]int),
		maxRetries:       config.MaxRetries,
		retryDelay:       config.RetryDelay,
		connectTimeout:   config.ConnectTimeout,
		requestTimeout:   config.RequestTimeout,
		failureThreshold: config.FailureThreshold,
		resetTimeout:     config.ResetTimeout,
	}
}

// GetClient returns a gRPC client for the given address
func (c *Client) GetClient(addr string) (pb.CacheServiceClient, error) {
	// Check circuit breaker
	if c.isCircuitOpen(addr) {
		return nil, fmt.Errorf("circuit breaker open for %s", addr)
	}
	
	// Try to get existing client
	c.mu.RLock()
	client, exists := c.clients[addr]
	conn, connExists := c.connections[addr]
	c.mu.RUnlock()
	
	// Check if connection is healthy
	if exists && connExists {
		state := conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			return client, nil
		}
	}
	
	// Need to create new connection
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Double-check after acquiring write lock
	if client, exists := c.clients[addr]; exists {
		if conn, exists := c.connections[addr]; exists {
			state := conn.GetState()
			if state == connectivity.Ready || state == connectivity.Idle {
				return client, nil
			}
		}
	}
	
	// Close old connection if exists
	if conn, exists := c.connections[addr]; exists {
		conn.Close()
	}
	
	// Create new connection
	ctx, cancel := context.WithTimeout(context.Background(), c.connectTimeout)
	defer cancel()
	
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
			grpc.MaxCallSendMsgSize(100*1024*1024), // 100MB
		),
	)
	if err != nil {
		c.recordFailure(addr)
		return nil, fmt.Errorf("failed to create client for %s: %w", addr, err)
	}
	// NewClient connects lazily; eagerly establish the connection within the
	// caller-supplied timeout so we fail fast on unreachable peers.
	conn.Connect()
	if !conn.WaitForStateChange(ctx, conn.GetState()) {
		conn.Close()
		c.recordFailure(addr)
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, ctx.Err())
	}
	
	client = pb.NewCacheServiceClient(conn)
	c.connections[addr] = conn
	c.clients[addr] = client
	
	return client, nil
}

// Get performs a Get RPC with retries
func (c *Client) Get(addr string, req *pb.GetRequest) (*pb.GetResponse, error) {
	c.totalRequests++
	
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}
		
		client, err := c.GetClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		resp, err := client.Get(ctx, req)
		cancel()
		
		if err == nil {
			c.recordSuccess(addr)
			return resp, nil
		}
		
		lastErr = err
	}
	
	c.failedRequests++
	c.recordFailure(addr)
	return nil, fmt.Errorf("get failed after %d attempts: %w", c.maxRetries+1, lastErr)
}

// Put performs a Put RPC with retries
func (c *Client) Put(addr string, req *pb.SetRequest) (*pb.SetResponse, error) {
	c.totalRequests++
	
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}
		
		client, err := c.GetClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		resp, err := client.Set(ctx, req)
		cancel()
		
		if err == nil {
			c.recordSuccess(addr)
			return resp, nil
		}
		
		lastErr = err
	}
	
	c.failedRequests++
	c.recordFailure(addr)
	return nil, fmt.Errorf("put failed after %d attempts: %w", c.maxRetries+1, lastErr)
}

// Replicate performs a Replicate RPC with retries
func (c *Client) Replicate(addr string, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	c.totalRequests++
	
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}
		
		client, err := c.GetClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		resp, err := client.Replicate(ctx, req)
		cancel()
		
		if err == nil {
			c.recordSuccess(addr)
			return resp, nil
		}
		
		lastErr = err
	}
	
	c.failedRequests++
	c.recordFailure(addr)
	return nil, fmt.Errorf("replicate failed after %d attempts: %w", c.maxRetries+1, lastErr)
}

// Heartbeat sends a heartbeat to a peer
func (c *Client) Heartbeat(addr string, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	client, err := c.GetClient(addr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := client.Heartbeat(ctx, req)
	if err != nil {
		c.recordFailure(addr)
		return nil, err
	}
	
	c.recordSuccess(addr)
	return resp, nil
}

// Invalidate sends an invalidation message
func (c *Client) Invalidate(addr string, req *pb.InvalidateRequest) (*pb.InvalidateResponse, error) {
	client, err := c.GetClient(addr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return client.Invalidate(ctx, req)
}


// Circuit breaker methods

func (c *Client) isCircuitOpen(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	failures, exists := c.failures[addr]
	if !exists {
		return false
	}
	
	return failures >= c.failureThreshold
}

func (c *Client) recordSuccess(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Reset failure count on success
	delete(c.failures, addr)
}

func (c *Client) recordFailure(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.failures[addr]++
	
	// Schedule circuit breaker reset
	if c.failures[addr] == c.failureThreshold {
		go c.scheduleReset(addr)
	}
}

func (c *Client) scheduleReset(addr string) {
	time.Sleep(c.resetTimeout)
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Reset circuit breaker
	delete(c.failures, addr)
}

// Close closes all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for addr, conn := range c.connections {
		conn.Close()
		delete(c.connections, addr)
		delete(c.clients, addr)
	}
	
	return nil
}

// GetStats returns client statistics
func (c *Client) GetStats() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return map[string]int64{
		"total_requests":  c.totalRequests,
		"failed_requests": c.failedRequests,
		"active_connections": int64(len(c.connections)),
	}
}

// HealthCheck checks if a connection to addr is healthy
func (c *Client) HealthCheck(addr string) bool {
	c.mu.RLock()
	conn, exists := c.connections[addr]
	c.mu.RUnlock()
	
	if !exists {
		return false
	}
	
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// CloseConnection closes a specific connection
func (c *Client) CloseConnection(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if conn, exists := c.connections[addr]; exists {
		conn.Close()
		delete(c.connections, addr)
		delete(c.clients, addr)
	}
	
	return nil
}