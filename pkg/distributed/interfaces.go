package distributed

// ShardInfo holds basic information about a shard for repair logic.
type ShardInfo struct {
	Index    int
	Type     string // "data" or "parity"
	Size     int64
	Checksum uint32
}

// ErasureProvider defines the interface required by distributed components 
// to perform erasure coding operations without depending on the erasure package.
type ErasureProvider interface {
	Encode(data []byte) ([][]byte, error)
	Decode(shards [][]byte) ([]byte, error)
	GetDataShards() int
	GetParityShards() int
	GetTotalShards() int
}
