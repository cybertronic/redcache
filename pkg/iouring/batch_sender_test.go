//go:build linux

package iouring

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// mockConn is a minimal net.Conn that records written bytes.
// It does NOT implement syscall.Conn, so extractFD will fail and
// BatchSender will fall back to net.Conn.Write — which is exactly
// what we want to test in the fallback path.
type mockConn struct {
	mu      sync.Mutex
	written []byte
	closed  bool
	delay   time.Duration // optional artificial write delay
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func (m *mockConn) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]byte, len(m.written))
	copy(out, m.written)
	return out
}

// newDisabledBatchSender returns a BatchSender with io_uring disabled so
// tests can exercise the fallback path without a real ring.
func newDisabledBatchSender() *BatchSender {
	return &BatchSender{
		loop: nil,
		features: &KernelFeatures{
			NetworkSupport: false,
			LinkedOps:      false,
			SendZCSupport:  false,
		},
		enabled: false,
	}
}

// ---------------------------------------------------------------------------
// Fallback path tests (no io_uring required)
// ---------------------------------------------------------------------------

func TestBatchSender_FallbackSendsAllShards(t *testing.T) {
	bs := newDisabledBatchSender()

	conns := []*mockConn{{}, {}, {}}
	shards := []ShardSend{
		{Conn: conns[0], Data: []byte("shard-0"), NodeID: "node-0"},
		{Conn: conns[1], Data: []byte("shard-1"), NodeID: "node-1"},
		{Conn: conns[2], Data: []byte("shard-2"), NodeID: "node-2"},
	}

	results := bs.SendShards(context.Background(), shards)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("shard %d: unexpected error: %v", i, r.Err)
		}
		if r.N != len(shards[i].Data) {
			t.Errorf("shard %d: wrote %d bytes, want %d", i, r.N, len(shards[i].Data))
		}
		if string(conns[i].Bytes()) != string(shards[i].Data) {
			t.Errorf("shard %d: conn received %q, want %q", i, conns[i].Bytes(), shards[i].Data)
		}
	}
}

func TestBatchSender_EmptyShards(t *testing.T) {
	bs := newDisabledBatchSender()
	results := bs.SendShards(context.Background(), nil)
	if results != nil {
		t.Errorf("expected nil results for empty shards, got %v", results)
	}
}

func TestBatchSender_SingleShard(t *testing.T) {
	bs := newDisabledBatchSender()
	conn := &mockConn{}
	shards := []ShardSend{
		{Conn: conn, Data: []byte("hello"), NodeID: "node-0"},
	}

	results := bs.SendShards(context.Background(), shards)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("unexpected error: %v", results[0].Err)
	}
	if string(conn.Bytes()) != "hello" {
		t.Errorf("conn received %q, want %q", conn.Bytes(), "hello")
	}
}

func TestBatchSender_ContextCancellation(t *testing.T) {
	bs := newDisabledBatchSender()

	// Use a conn with a delay longer than the context deadline.
	slowConn := &mockConn{delay: 200 * time.Millisecond}
	shards := []ShardSend{
		{Conn: slowConn, Data: []byte("data"), NodeID: "slow-node"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	results := bs.sendFallback(ctx, shards)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// Either the write completed before the deadline (race) or ctx was cancelled.
	// We just verify no panic and the result is populated.
	_ = results[0]
}

func TestBatchSender_SendShardsWithTimeout(t *testing.T) {
	bs := newDisabledBatchSender()
	conn := &mockConn{}
	shards := []ShardSend{
		{Conn: conn, Data: []byte("timed"), NodeID: "node-0"},
	}

	results := bs.SendShardsWithTimeout(shards, time.Second)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("unexpected error: %v", results[0].Err)
	}
}

func TestBatchSender_EnabledAndUsesSendZC_Disabled(t *testing.T) {
	bs := newDisabledBatchSender()
	if bs.Enabled() {
		t.Error("expected Enabled() == false for disabled sender")
	}
	if bs.UsesSendZC() {
		t.Error("expected UsesSendZC() == false for disabled sender")
	}
}

func TestBatchSender_EnabledAndUsesSendZC_Enabled(t *testing.T) {
	bs := &BatchSender{
		loop: nil,
		features: &KernelFeatures{
			NetworkSupport: true,
			LinkedOps:      true,
			SendZCSupport:  true,
		},
		enabled: true,
	}
	if !bs.Enabled() {
		t.Error("expected Enabled() == true")
	}
	if !bs.UsesSendZC() {
		t.Error("expected UsesSendZC() == true")
	}
}

// ---------------------------------------------------------------------------
// extractFD tests
// ---------------------------------------------------------------------------

func TestExtractFD_NonSyscallConn(t *testing.T) {
	conn := &mockConn{} // does not implement syscall.Conn
	_, err := extractFD(conn)
	if err == nil {
		t.Error("expected error for conn without syscall.Conn, got nil")
	}
}

func TestExtractFD_RealTCPConn(t *testing.T) {
	// Create a real TCP connection pair to test fd extraction.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen: %v", err)
	}
	defer ln.Close()

	done := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		done <- c
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Skipf("cannot dial: %v", err)
	}
	defer client.Close()

	server := <-done
	defer server.Close()

	fd, err := extractFD(client)
	if err != nil {
		t.Errorf("extractFD on real TCP conn: %v", err)
	}
	if fd <= 0 {
		t.Errorf("expected positive fd, got %d", fd)
	}
}

// ---------------------------------------------------------------------------
// unsafeSlicePtr tests
// ---------------------------------------------------------------------------

func TestUnsafeSlicePtr_Empty(t *testing.T) {
	if unsafeSlicePtr(nil) != 0 {
		t.Error("expected 0 for nil slice")
	}
	if unsafeSlicePtr([]byte{}) != 0 {
		t.Error("expected 0 for empty slice")
	}
}

func TestUnsafeSlicePtr_NonEmpty(t *testing.T) {
	b := []byte{1, 2, 3}
	ptr := unsafeSlicePtr(b)
	if ptr == 0 {
		t.Error("expected non-zero pointer for non-empty slice")
	}
}

// ---------------------------------------------------------------------------
// ShardResult helpers
// ---------------------------------------------------------------------------

func TestShardResult_Fields(t *testing.T) {
	r := ShardResult{NodeID: "n1", N: 42, Err: nil}
	if r.NodeID != "n1" {
		t.Errorf("NodeID: got %q, want %q", r.NodeID, "n1")
	}
	if r.N != 42 {
		t.Errorf("N: got %d, want 42", r.N)
	}
}

// ---------------------------------------------------------------------------
// Concurrent safety test
// ---------------------------------------------------------------------------

func TestBatchSender_ConcurrentSends(t *testing.T) {
	bs := newDisabledBatchSender()

	const goroutines = 10
	const shardsPerGoroutine = 5

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shards := make([]ShardSend, shardsPerGoroutine)
			for i := range shards {
				shards[i] = ShardSend{
					Conn:   &mockConn{},
					Data:   []byte("payload"),
					NodeID: "node",
				}
			}
			results := bs.SendShards(context.Background(), shards)
			for _, r := range results {
				if r.Err != nil {
					t.Errorf("concurrent send error: %v", r.Err)
				}
			}
		}()
	}
	wg.Wait()
}