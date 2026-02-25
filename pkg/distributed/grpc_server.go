package distributed

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	
	pb "redcache/proto"
	"redcache/pkg/cache"
)

// GRPCServer implements the gRPC server for cache operations
type GRPCServer struct {
	pb.UnimplementedCacheServiceServer
	
	config         *ClusterConfig
	storage        *cache.Storage
	clusterManager *ClusterManager
	
	server  *grpc.Server
	running bool
	mu      sync.RWMutex
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(config *ClusterConfig, storage *cache.Storage, cm *ClusterManager) *GRPCServer {
	return &GRPCServer{
		config:         config,
		storage:        storage,
		clusterManager: cm,
	}
}

// Start starts the gRPC server
func (s *GRPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("gRPC server already running")
	}

	// Create listener
	addr := fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.RPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Configure gRPC server options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(100 * 1024 * 1024), // 100MB
		grpc.MaxSendMsgSize(100 * 1024 * 1024), // 100MB
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Second * 30,
			Timeout: time.Second * 10,
		}),
	}

	// Add TLS if configured
	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(
			s.config.TLSCertFile,
			s.config.TLSKeyFile,
		)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Create gRPC server
	s.server = grpc.NewServer(opts...)
	pb.RegisterCacheServiceServer(s.server, s)

	// Start serving
	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	s.running = true
	return nil
}

// Shutdown gracefully shuts down the gRPC server
func (s *GRPCServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	// Graceful stop with timeout
	stopped := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.running = false
		return nil
	case <-ctx.Done():
		s.server.Stop() // Force stop
		s.running = false
		return ctx.Err()
	}
}

// Get implements the Get RPC method
func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.storage.Get(ctx, req.Key)
	
	return &pb.GetResponse{
		Found: found,
		Value: value,
	}, nil
}

// Set implements the Set RPC method
func (s *GRPCServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	err := s.storage.Put(ctx, req.Key, req.Value)
	
	return &pb.SetResponse{
		Success: err == nil,
		Error:   errorString(err),
	}, nil
}

// Delete implements the Delete RPC method
func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.storage.Delete(ctx, req.Key)
	
	return &pb.DeleteResponse{
		Success: err == nil,
		Error:   errorString(err),
	}, nil
}

// Replicate implements the Replicate RPC method
func (s *GRPCServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	err := s.storage.Put(ctx, req.Key, req.Value)
	
	return &pb.ReplicateResponse{
		Success: err == nil,
		Error:   errorString(err),
	}, nil
}

// Heartbeat implements the Heartbeat RPC method
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{
		Acknowledged: true,
		Timestamp:    time.Now().Unix(),
	}, nil
}

// Ping implements the Ping RPC method
func (s *GRPCServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Alive:     true,
		Timestamp: time.Now().Unix(),
	}, nil
}

// Invalidate implements the Invalidate RPC method
func (s *GRPCServer) Invalidate(ctx context.Context, req *pb.InvalidateRequest) (*pb.InvalidateResponse, error) {
	err := s.storage.Delete(ctx, req.Key)
	
	return &pb.InvalidateResponse{
		Success: err == nil,
		Error:   errorString(err),
	}, nil
}

// GetBatch implements the GetBatch RPC method
func (s *GRPCServer) GetBatch(ctx context.Context, req *pb.GetBatchRequest) (*pb.GetBatchResponse, error) {
	values := make(map[string][]byte)
	missingKeys := make([]string, 0)
	
	for _, key := range req.Keys {
		value, found := s.storage.Get(ctx, key)
		if found {
			values[key] = value
		} else {
			missingKeys = append(missingKeys, key)
		}
	}
	
	return &pb.GetBatchResponse{
		Values:      values,
		MissingKeys: missingKeys,
	}, nil
}

// SetBatch implements the SetBatch RPC method
func (s *GRPCServer) SetBatch(ctx context.Context, req *pb.SetBatchRequest) (*pb.SetBatchResponse, error) {
	failedKeys := make([]string, 0)
	
	for key, value := range req.Entries {
		if err := s.storage.Put(ctx, key, value); err != nil {
			failedKeys = append(failedKeys, key)
		}
	}
	
	return &pb.SetBatchResponse{
		Success:    len(failedKeys) == 0,
		FailedKeys: failedKeys,
	}, nil
}

// IsRunning returns whether the server is running
func (s *GRPCServer) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// errorString converts an error to a string, handling nil
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}