package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	
	pb "redcache/proto"
)

// GRPCClient implements the RPC client using gRPC
type GRPCClient struct {
	config      *ClusterConfig
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.CacheServiceClient
	mu          sync.RWMutex
}

// NewGRPCClient creates a new gRPC client
func NewGRPCClient(config *ClusterConfig) *GRPCClient {
	return &GRPCClient{
		config:      config,
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.CacheServiceClient),
	}
}

// getClient returns a gRPC client for the given address
func (c *GRPCClient) getClient(addr string) (pb.CacheServiceClient, error) {
	c.mu.RLock()
	client, exists := c.clients[addr]
	c.mu.RUnlock()
	
	if exists {
		return client, nil
	}
	
	// Create new connection
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Double-check after acquiring write lock
	if client, exists := c.clients[addr]; exists {
		return client, nil
	}
	
	// Configure dial options
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100 * 1024 * 1024), // 100MB
			grpc.MaxCallSendMsgSize(100 * 1024 * 1024), // 100MB
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 30,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
	
	// Add TLS if configured
	if c.config.TLSCertFile != "" {
		creds, err := credentials.NewClientTLSFromFile(c.config.TLSCertFile, "")
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	
	// Create connection
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", addr, err)
	}
	
	// Create client
	client = pb.NewCacheServiceClient(conn)
	c.connections[addr] = conn
	c.clients[addr] = client
	
	return client, nil
}

// Get retrieves a value from a remote node (interface method)
func (c *GRPCClient) Get(addr string, req *pb.GetRequest) (*pb.GetResponse, error) {
	client, err := c.getClient(addr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	
	return client.Get(ctx, req)
}

// Replicate replicates data to a remote node (interface method)
func (c *GRPCClient) Replicate(addr string, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	client, err := c.getClient(addr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	
	return client.Replicate(ctx, req)
}

// Heartbeat sends a heartbeat to a remote node (interface method)
func (c *GRPCClient) Heartbeat(addr string, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	client, err := c.getClient(addr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	
	return client.Heartbeat(ctx, req)
}

// GetValue retrieves a value from a remote node (helper method)
func (c *GRPCClient) GetValue(ctx context.Context, addr string, port int, key string) ([]byte, error) {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	req := &pb.GetRequest{
		Key:       key,
		NodeId:    c.config.NodeID,
		Timestamp: time.Now().Unix(),
	}
	
	resp, err := c.Get(fullAddr, req)
	if err != nil {
		return nil, err
	}
	
	if !resp.Found {
		return nil, fmt.Errorf("key not found")
	}
	
	return resp.Value, nil
}

// SetValue stores a value on a remote node (helper method)
func (c *GRPCClient) SetValue(ctx context.Context, addr string, port int, key string, value []byte, ttl time.Duration) error {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	client, err := c.getClient(fullAddr)
	if err != nil {
		return err
	}
	
	req := &pb.SetRequest{
		Key:        key,
		Value:      value,
		TtlSeconds: int64(ttl.Seconds()),
		NodeId:     c.config.NodeID,
		Timestamp:  time.Now().Unix(),
	}
	
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	
	resp, err := client.Set(ctx, req)
	if err != nil {
		return err
	}
	
	if !resp.Success {
		return fmt.Errorf("set failed: %s", resp.Error)
	}
	
	return nil
}

// DeleteValue removes a value from a remote node (helper method)
func (c *GRPCClient) DeleteValue(ctx context.Context, addr string, port int, key string) error {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	client, err := c.getClient(fullAddr)
	if err != nil {
		return err
	}
	
	req := &pb.DeleteRequest{
		Key:       key,
		NodeId:    c.config.NodeID,
		Timestamp: time.Now().Unix(),
	}
	
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return err
	}
	
	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}
	
	return nil
}

// PingNode checks if a remote node is alive (helper method)
func (c *GRPCClient) PingNode(ctx context.Context, addr string, port int) error {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	client, err := c.getClient(fullAddr)
	if err != nil {
		return err
	}
	
	req := &pb.PingRequest{
		NodeId:    c.config.NodeID,
		Timestamp: time.Now().Unix(),
	}
	
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	
	resp, err := client.Ping(ctx, req)
	if err != nil {
		return err
	}
	
	if !resp.Alive {
		return fmt.Errorf("node not alive")
	}
	
	return nil
}

// GetBatch retrieves multiple values from a remote node
func (c *GRPCClient) GetBatch(ctx context.Context, addr string, port int, keys []string) (map[string][]byte, error) {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	client, err := c.getClient(fullAddr)
	if err != nil {
		return nil, err
	}
	
	req := &pb.GetBatchRequest{
		Keys:      keys,
		NodeId:    c.config.NodeID,
		Timestamp: time.Now().Unix(),
	}
	
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	
	resp, err := client.GetBatch(ctx, req)
	if err != nil {
		return nil, err
	}
	
	return resp.Values, nil
}

// SetBatch stores multiple values on a remote node
func (c *GRPCClient) SetBatch(ctx context.Context, addr string, port int, entries map[string][]byte, ttl time.Duration) error {
	fullAddr := fmt.Sprintf("%s:%d", addr, port)
	
	client, err := c.getClient(fullAddr)
	if err != nil {
		return err
	}
	
	req := &pb.SetBatchRequest{
		Entries:    entries,
		TtlSeconds: int64(ttl.Seconds()),
		NodeId:     c.config.NodeID,
		Timestamp:  time.Now().Unix(),
	}
	
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	
	resp, err := client.SetBatch(ctx, req)
	if err != nil {
		return err
	}
	
	if !resp.Success {
		return fmt.Errorf("batch set failed for keys: %v", resp.FailedKeys)
	}
	
	return nil
}

// Close closes all connections
func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	var errs []error
	for addr, conn := range c.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection to %s: %w", addr, err))
		}
	}
	
	c.connections = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]pb.CacheServiceClient)
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	
	return nil
}