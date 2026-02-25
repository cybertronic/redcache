package rpc

import (
	pb "redcache/proto"
)

// RPCClient defines the interface for RPC client operations
type RPCClient interface {
	Get(addr string, req *pb.GetRequest) (*pb.GetResponse, error)
	Put(addr string, req *pb.SetRequest) (*pb.SetResponse, error)
	Replicate(addr string, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error)
	Heartbeat(addr string, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
	// JoinCluster(addr string, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error)
	Close() error
}