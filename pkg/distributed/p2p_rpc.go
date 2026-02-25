package distributed

import (
	"context"
	"fmt"
	"time"

	pb "redcache/proto"
)

// RPCClient interface for P2P communication (avoids import cycle)
type P2PRPCClient interface {
	Get(addr string, req *pb.GetRequest) (*pb.GetResponse, error)
	Replicate(addr string, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error)
	Heartbeat(addr string, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
}

// SetRPCClient sets the RPC client for P2P communication
func (p2p *P2PCache) SetRPCClient(client P2PRPCClient) {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()
	
	p2p.rpcClient = client
}

// requestFromPeerRPC requests data from a peer using RPC
func (p2p *P2PCache) requestFromPeerRPC(ctx context.Context, peer *Peer, key string) ([]byte, error) {
	if p2p.rpcClient == nil {
		return nil, fmt.Errorf("RPC client not initialized")
	}
	
	startTime := time.Now()
	
	// Make actual RPC call
	req := &pb.GetRequest{
		Key:    key,
		NodeId: p2p.config.NodeID,
	}
	
	resp, err := p2p.rpcClient.Get(peer.Address, req)
	if err != nil {
		return nil, fmt.Errorf("RPC get failed: %w", err)
	}
	
	// Update peer RTT
	peer.RTT = time.Since(startTime)
	
	if !resp.Found {
		return nil, fmt.Errorf("key not found on peer")
	}
	
	p2p.stats.DataTransferred += int64(len(resp.Value))
	
	return resp.Value, nil
}

// replicateToPeerRPC replicates data to a peer using RPC
func (p2p *P2PCache) replicateToPeerRPC(ctx context.Context, peer *Peer, key string, value []byte) error {
	if p2p.rpcClient == nil {
		return fmt.Errorf("RPC client not initialized")
	}
	
	startTime := time.Now()
	
	// Make actual RPC call
	req := &pb.ReplicateRequest{
		Key:        key,
		Value:      value,
		SourceNode: p2p.config.NodeID,
	}
	
	resp, err := p2p.rpcClient.Replicate(peer.Address, req)
	if err != nil {
		return fmt.Errorf("RPC replicate failed: %w", err)
	}
	
	// Update peer RTT
	peer.RTT = time.Since(startTime)
	
	if !resp.Success {
		return fmt.Errorf("replication failed: %s", resp.Error)
	}
	
	p2p.stats.DataTransferred += int64(len(value))
	
	return nil
}

// SendHeartbeatRPC sends a heartbeat to a peer using RPC
func (p2p *P2PCache) SendHeartbeatRPC(peer *Peer) error {
	if p2p.rpcClient == nil {
		return fmt.Errorf("RPC client not initialized")
	}
	
	req := &pb.HeartbeatRequest{
		NodeId:    p2p.config.NodeID,
		Timestamp: time.Now().Unix(),
	}
	
	resp, err := p2p.rpcClient.Heartbeat(peer.Address, req)
	if err != nil {
		return err
	}
	
	if resp.Acknowledged {
		peer.LastSeen = time.Now()
	}
	
	return nil
}

// UpdatePeerHeartbeat updates the last seen time for a peer
func (p2p *P2PCache) UpdatePeerHeartbeat(peerID string, timestamp time.Time) {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()
	
	if peer, exists := p2p.peers[peerID]; exists {
		peer.LastSeen = timestamp
		peer.State = PeerStateConnected
	}
}

// GetActivePeers returns all active peers
func (p2p *P2PCache) GetActivePeers() []*Peer {
	p2p.mu.RLock()
	defer p2p.mu.RUnlock()
	
	peers := make([]*Peer, 0)
	for _, peer := range p2p.peers {
		if peer.State == PeerStateConnected {
			peers = append(peers, peer)
		}
	}
	
	return peers
}

// HandleReplication handles incoming replication requests
func (p2p *P2PCache) HandleReplication(ctx context.Context, key string, value []byte, version int64) error {
	// Store replicated data locally
	p2p.putLocal(key, value)
	
	return nil
}
