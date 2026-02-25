package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "redcache/proto"
	"redcache/pkg/cache"
	"redcache/pkg/distributed"
)

// Server implements the gRPC server for distributed cache operations
type Server struct {
	pb.UnimplementedCacheServiceServer
	
	// Core components
	cache      *cache.Storage
	p2p        *distributed.P2PCache
	coherence  *distributed.CoherenceManager
	quorum     *distributed.QuorumManager
	
	// Server state
	nodeID     string
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	
	// Synchronization
	mu         sync.RWMutex
	stopChan   chan struct{}
	
	// Metrics
	requestCount int64
	errorCount   int64
}

// ServerConfig configures the RPC server
type ServerConfig struct {
	NodeID    string
	Address   string
	Cache     *cache.Storage
	P2P       *distributed.P2PCache
	Coherence *distributed.CoherenceManager
	Quorum    *distributed.QuorumManager
}

// NewServer creates a new RPC server
func NewServer(config ServerConfig) *Server {
	return &Server{
		cache:     config.Cache,
		p2p:       config.P2P,
		coherence: config.Coherence,
		quorum:    config.Quorum,
		nodeID:    config.NodeID,
		address:   config.Address,
		stopChan:  make(chan struct{}),
	}
}

// Start starts the gRPC server
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis
	
	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(100 * 1024 * 1024), // 100MB
		grpc.MaxSendMsgSize(100 * 1024 * 1024), // 100MB
		grpc.ConnectionTimeout(30 * time.Second),
	}
	
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterCacheServiceServer(s.grpcServer, s)
	
	// Start serving
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()
	
	return nil
}

// Stop stops the gRPC server
func (s *Server) Stop() error {
	select {
	case <-s.stopChan:
		return nil // already closed
	default:
		close(s.stopChan)
	}
	
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	
	if s.listener != nil {
		s.listener.Close()
	}
	
	return nil
}

// Get handles cache get requests
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()
	
	// Check if we have quorum (if quorum is enabled)
	if s.quorum != nil && !s.quorum.HasQuorum() {
		return nil, status.Error(codes.Unavailable, "no quorum")
	}
	
	// Get from local cache
	value, exists := s.cache.Get(ctx, req.Key)
	if !exists {
		return &pb.GetResponse{Found: false}, nil
	}
	
	// Get cache state from coherence manager if available
// 	var cacheState pb.CacheState
// 	if s.coherence != nil {
// 		state := s.coherence.GetState(req.Key)
// 		cacheState = convertCacheState(state)
// 	}
// 	
// 	return &pb.GetResponse{
// 		Found: true,
// 		Value: value,
// 		State: cacheState,
// 	}, nil
	return &pb.GetResponse{Found: true, Value: value}, nil
}
// }

// Set handles cache set requests
func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()
	
	// Check if we have quorum for writes
	if s.quorum != nil && !s.quorum.HasQuorum() {
		return &pb.SetResponse{
			Success: false,
			Error:   "no quorum for writes",
		}, nil
	}
	
	// Check if we're in read-only mode
	if s.quorum != nil && s.quorum.IsReadOnly() {
		return &pb.SetResponse{
			Success: false,
			Error:   "node in read-only mode (minority partition)",
		}, nil
	}
	
	// Put in local cache
	err := s.cache.Put(ctx, req.Key, req.Value)
	if err != nil {
		s.mu.Lock()
		s.errorCount++
		s.mu.Unlock()
		return &pb.SetResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	// Update coherence state if available
	if s.coherence != nil {
		// s.coherence.UpdateState(req.Key, distributed.StateModified)
	}
	
	return &pb.SetResponse{
		Success: true,
	}, nil
}

// Delete handles cache delete requests
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()
	
	// Check quorum
	if s.quorum != nil && !s.quorum.HasQuorum() {
		return &pb.DeleteResponse{
			Success: false,
			Error:   "no quorum",
		}, nil
	}
	
	// Delete from local cache
	err := s.cache.Delete(ctx, req.Key)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	// Invalidate in coherence manager
	if s.coherence != nil {
			_ = s.coherence.Invalidate(ctx, req.Key)
	}
	
	return &pb.DeleteResponse{
		Success: true,
	}, nil
}

// Replicate handles replication requests
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	// Store replicated data
	err := s.cache.Put(ctx, req.Key, req.Value)
	if err != nil {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	// Mark as shared in coherence
	if s.coherence != nil {
		// s.coherence.UpdateState(req.Key, distributed.StateInvalid)
	}
	
	return &pb.ReplicateResponse{
		Success: true,
	}, nil
}

// // SyncData handles data synchronization requests
// func (s *Server) SyncData(ctx context.Context, req *pb.SyncDataRequest) (*pb.SyncDataResponse, error) {
// 	entries := make([]*pb.CacheEntry, 0)
// 	
// 	// Get requested keys
// 	for _, key := range req.Keys {
// 		value, exists := s.cache.Get(ctx, key)
// 		if !exists {
// 			continue
// 		}
// 		
// 		entry := &pb.CacheEntry{
// 			Key:   key,
// 			Value: value,
// 		}
// 		entries = append(entries, entry)
// 	}
// 	
// 	return &pb.SyncDataResponse{
// 		Entries:      entries,
// 		TotalEntries: int64(len(entries)),
// 		HasMore:      false,
// }, nil
// }

// Invalidate handles cache invalidation requests
func (s *Server) Invalidate(ctx context.Context, req *pb.InvalidateRequest) (*pb.InvalidateResponse, error) {
	if s.coherence != nil {
			_ = s.coherence.Invalidate(ctx, req.Key)
	}
	
	return &pb.InvalidateResponse{
		Success: true,
	}, nil
}

// // Upgrade handles cache state upgrade requests
// func (s *Server) Upgrade(ctx context.Context, req *pb.UpgradeRequest) (*pb.UpgradeResponse, error) {
// 	if s.coherence == nil {
// 		return &pb.UpgradeResponse{
// 			Success: false,
// 			Error:   "coherence not enabled",
// 		}, nil
// 	}
// 	
// 	// Perform upgrade
// 	toState := convertToInternalState(req.ToState)
// 	err := s.coherence.Upgrade(req.Key, toState)
// 	if err != nil {
// 		return &pb.UpgradeResponse{
// 			Success: false,
// 			Error:   err.Error(),
// 		}, nil
// 	}
// 	
// 	currentState := s.coherence.GetState(req.Key)
// 	
// 	return &pb.UpgradeResponse{
// 		Success:      true,
// 		CurrentState: convertCacheState(currentState),
// 	}, nil
// }

// // Downgrade handles cache state downgrade requests
// func (s *Server) Downgrade(ctx context.Context, req *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
// 	if s.coherence == nil {
// 		return &pb.DowngradeResponse{
// 			Success: false,
// 			Error:   "coherence not enabled",
// 		}, nil
// 	}
// 	
// 	// Perform downgrade
// 	toState := convertToInternalState(req.ToState)
// 	err := s.coherence.Downgrade(req.Key, toState)
// 	if err != nil {
// 		return &pb.DowngradeResponse{
// 			Success: false,
// 			Error:   err.Error(),
// 		}, nil
// 	}
// 	
// 	currentState := s.coherence.GetState(req.Key)
// 	
// 	return &pb.DowngradeResponse{
// 		Success:      true,
// 		CurrentState: convertCacheState(currentState),
// 	}, nil
// }
// 
// // Heartbeat handles heartbeat requests
// func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
// 	// Update node information
// 	if s.p2p != nil {
// 		s.p2p.UpdatePeerHeartbeat(req.NodeId, time.Now())
// 	}
// 	
// 	// Get active nodes
// 	activeNodes := make([]*pb.NodeInfo, 0)
// 	if s.p2p != nil {
// 		peers := s.p2p.GetActivePeers()
// 		for _, peer := range peers {
// 			nodeInfo := &pb.NodeInfo{
// 				NodeId:        peer.ID,
// 				Address:       peer.Address,
// 				LastHeartbeat: peer.LastSeen.Unix(),
// 			}
// 			activeNodes = append(activeNodes, nodeInfo)
// 		}
// 	}
// 	
// 	return &pb.HeartbeatResponse{
// 		Acknowledged: true,
// 		Timestamp:    time.Now().Unix(),
// 		ActiveNodes:  activeNodes,
// 	}, nil
// }
// 
// // JoinCluster handles cluster join requests
// // func (s *Server) JoinCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
// // 	if s.p2p == nil {
// // 		return &pb.JoinClusterResponse{
// 			Success: false,
// 			Error:   "P2P not enabled",
// 		}, nil
// 	}
// 	
// 	// Add peer
// 	peer := &distributed.Peer{
// 		ID:      req.NodeId,
// 		Address: req.Address,
// 		State:   distributed.PeerStateConnected,
// 	}
// 	
// 	err := s.p2p.AddPeer(peer)
// 	if err != nil {
// 		return &pb.JoinClusterResponse{
// 			Success: false,
// 			Error:   err.Error(),
// 		}, nil
// 	}
// 	
// 	// Get cluster nodes
// 	clusterNodes := make([]*pb.NodeInfo, 0)
// 	peers := s.p2p.GetActivePeers()
// 	for _, p := range peers {
// 		nodeInfo := &pb.NodeInfo{
// 			NodeId:  p.ID,
// 			Address: p.Address,
// 		}
// 		clusterNodes = append(clusterNodes, nodeInfo)
// 	}
// 	
// 	return &pb.JoinClusterResponse{
// 		Success:      true,
// 		ClusterNodes: clusterNodes,
// 	}, nil
// }
// 
// LeaveCluster handles cluster leave requests
// func (s *Server) LeaveCluster(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
// 	if s.p2p == nil {
// 		return &pb.LeaveClusterResponse{
// 			Success: false,
// 			Error:   "P2P not enabled",
// 		}, nil
// 	}
// 	
// 	// Remove peer
// 	s.p2p.RemovePeer(req.NodeId)
// 	
// 	return &pb.LeaveClusterResponse{
// 		Success: true,
// 	}, nil
// }
// 
// // QuorumCheck handles quorum check requests
// func (s *Server) QuorumCheck(ctx context.Context, req *pb.QuorumCheckRequest) (*pb.QuorumCheckResponse, error) {
// 	if s.quorum == nil {
// 		return &pb.QuorumCheckResponse{
// 			HasQuorum: true, // If quorum not enabled, always return true
// 		}, nil
// 	}
// 	
// 	hasQuorum := s.quorum.HasQuorum()
// 	activeNodes := s.quorum.GetActiveNodeCount()
// 	requiredNodes := s.quorum.GetQuorumSize()
// 	isPrimary := s.quorum.IsPrimaryPartition()
// 	
// 	return &pb.QuorumCheckResponse{
// 		HasQuorum:          hasQuorum,
// 		ActiveNodes:        int32(activeNodes),
// 		RequiredNodes:      int32(requiredNodes),
// 		IsPrimaryPartition: isPrimary,
// 	}, nil
// }
// 
// PartitionStatus handles partition status requests
// func (s *Server) PartitionStatus(ctx context.Context, req *pb.PartitionStatusRequest) (*pb.PartitionStatusResponse, error) {
// 	if s.quorum == nil {
// 		return &pb.PartitionStatusResponse{
// 			PartitionDetected: false,
// 		}, nil
// 	}
// 	
// 	partitionDetected := s.quorum.IsPartitioned()
// 	
// 	return &pb.PartitionStatusResponse{
// 		PartitionDetected: partitionDetected,
// 	}, nil
// }
// 
// // ResolveConflict handles conflict resolution requests
// // func (s *Server) ResolveConflict(ctx context.Context, req *pb.ResolveConflictRequest) (*pb.ResolveConflictResponse, error) {
// // 	// Simple last-write-wins for now
// // 	if len(req.ConflictingEntries) == 0 {
// // 		return &pb.ResolveConflictResponse{
// // 			Success: false,
// // 			Error:   "no conflicting entries provided",
// // 		}, nil
// // 	}
// // 	
// 	// Find entry with highest timestamp
// 	var resolved *pb.CacheEntry
// 	maxTimestamp := int64(0)
// 	
// 	for _, entry := range req.ConflictingEntries {
// 		if entry.Timestamp > maxTimestamp {
// 			maxTimestamp = entry.Timestamp
// 			resolved = entry
// 		}
// 	}
// 	
// 	return &pb.ResolveConflictResponse{
// 		Success:       true,
// 		ResolvedEntry: resolved,
// 	}, nil
// }

// Helper functions

// func convertCacheState(state distributed.CacheState) pb.CacheState {
// 	switch state {
// 	case distributed.StateInvalid:
// 		return pb.CacheState_INVALID
// 	case distributed.StateShared:
// 		return pb.CacheState_SHARED
// 	case distributed.StateExclusive:
// 		return pb.CacheState_EXCLUSIVE
// 	case distributed.StateModified:
// 		return pb.CacheState_MODIFIED
// 	case distributed.StateOwner:
// 		return pb.CacheState_OWNER
// 	default:
// 		return pb.CacheState_INVALID
// 	}
// }
// 
// func convertToInternalState(state pb.CacheState) distributed.CacheState {
// 	switch state {
// 	case pb.CacheState_INVALID:
// 		return distributed.StateInvalid
// 	case pb.CacheState_SHARED:
// 		return distributed.StateShared
// 	case pb.CacheState_EXCLUSIVE:
// 		return distributed.StateExclusive
// 	case pb.CacheState_MODIFIED:
// 		return distributed.StateModified
// 	case pb.CacheState_OWNER:
// 		return distributed.StateOwner
// 	default:
// 		return distributed.StateInvalid
// 	}
// }

// GetStats returns server statistics
func (s *Server) GetStats() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return map[string]int64{
		"request_count": s.requestCount,
		"error_count":   s.errorCount,
	}
}