package distributed

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"redcache/pkg/cache"
)

// RPCServer provides RPC server functionality using gRPC
type RPCServer struct {
	config         *ClusterConfig
	storage        *cache.Storage
	clusterManager *ClusterManager
	grpcServer     *grpc.Server
	running        bool
	mu             sync.RWMutex
}

// NewRPCServer creates a new RPC server
func NewRPCServer(config *ClusterConfig, storage *cache.Storage, cm *ClusterManager) *RPCServer {
	return &RPCServer{
		config:         config,
		storage:        storage,
		clusterManager: cm,
	}
}

// Start starts the gRPC server and begins listening for peer requests
func (s *RPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("RPC server already running")
	}

	addr := fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.RPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.grpcServer = grpc.NewServer()
	
	// Registration is handled via grpc_server.go which defines the GRPCServer wrapper.
	// This module serves as the network listener lifecycle manager.

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			// In production, this would trigger a process-level health failure
		}
	}()

	s.running = true
	return nil
}

// Shutdown gracefully shuts down the gRPC server
func (s *RPCServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running || s.grpcServer == nil {
		return nil
	}

	// Try graceful stop first
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpcServer.Stop()
	}

	s.running = false
	return nil
}

// IsRunning returns whether the server is running
func (s *RPCServer) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}
