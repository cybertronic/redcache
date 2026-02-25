package distributed

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

// CircuitState represents the state of the circuit breaker
type CircuitState int

const (
	StateClosed CircuitState = iota // S3 is healthy
	StateOpen                        // S3 is failing, block requests
	StateHalfOpen                    // Testing if S3 is healthy again
)

// FetchCoordinator coordinates cache fetches across the cluster to prevent thundering herd
type FetchCoordinator struct {
	// Request deduplication (per-node)
	fetchGroup singleflight.Group

	// Distributed coordination
	lockManager *LockManager
	gossip      *GossipProtocol

	// Rate limiting
	rateLimiter *rate.Limiter
	semaphore   chan struct{}

	// Circuit Breaker state
	circuitState CircuitState
	failureCount int64
	lastFailure  time.Time
	circuitMu    sync.RWMutex

	// Fetch status tracking
	fetchStatus map[string]*FetchStatus
	statusMu    sync.RWMutex

	// Configuration
	config FetchCoordinatorConfig

	// Metrics
	metrics FetchMetrics
	mu      sync.RWMutex

	// Callbacks
	fetchFunc FetchFunc
}

// FetchCoordinatorConfig configures the fetch coordinator
type FetchCoordinatorConfig struct {
	// Enable distributed coordination (uses locks)
	EnableDistributedCoordination bool

	// Rate limiting
	RequestsPerSecond    int           // Max requests per second per node
	MaxConcurrentFetches int           // Max concurrent fetches per node
	FetchTimeout         time.Duration // Timeout for fetch operations

	// Waiting
	MaxQueuedRequests int           // Max requests waiting for fetch
	QueueTimeout      time.Duration // Timeout for queued requests

	// Retry & Backoff (S3 Throttling Protection)
	MaxRetries      int
	RetryBackoff    time.Duration // Base backoff (e.g. 100ms)
	RetryMaxBackoff time.Duration // Max backoff (e.g. 10s)

	// Circuit Breaker Config
	CircuitFailureThreshold int64         // Trip after X failures
	CircuitBreakDuration    time.Duration // How long to wait before half-open
}

// FetchStatus tracks the status of an in-progress fetch
type FetchStatus struct {
	Key       string
	NodeID    string
	StartTime time.Time
	Progress  float64
	Size      int64
}

// FetchMetrics tracks fetch coordination metrics
type FetchMetrics struct {
	CoalescedRequests      int64
	CoordinatedRequests    int64
	QueuedRequests         int64
	RateLimitedRequests    int64
	ObjectStoreFetches     int64
	TotalWaitTime          time.Duration
	MaxQueueDepth          int
	CurrentQueueDepth      int
	FailedFetches          int64
	TimeoutFetches         int64
	CircuitBreakerTrips    int64
	CircuitBreakerBlocks   int64
}

// FetchFunc is the function that actually fetches data from the object store
type FetchFunc func(ctx context.Context, key string) ([]byte, error)

// NewFetchCoordinator creates a new fetch coordinator
func NewFetchCoordinator(config FetchCoordinatorConfig, lockManager *LockManager, gossip *GossipProtocol, fetchFunc FetchFunc) *FetchCoordinator {
	// Set defaults if not provided
	if config.CircuitFailureThreshold == 0 { config.CircuitFailureThreshold = 5 }
	if config.CircuitBreakDuration == 0 { config.CircuitBreakDuration = 30 * time.Second }

	fc := &FetchCoordinator{
		lockManager:  lockManager,
		gossip:       gossip,
		fetchStatus:  make(map[string]*FetchStatus),
		config:       config,
		fetchFunc:    fetchFunc,
		circuitState: StateClosed,
	}

	if config.RequestsPerSecond > 0 {
		fc.rateLimiter = rate.NewLimiter(rate.Limit(config.RequestsPerSecond), config.RequestsPerSecond)
	}

	if config.MaxConcurrentFetches > 0 {
		fc.semaphore = make(chan struct{}, config.MaxConcurrentFetches)
	}

	if gossip != nil {
		gossip.AddMessageHandler(MsgTypeFetchStarted, fc.handleGossipFetchStatus)
		gossip.AddMessageHandler(MsgTypeFetchCompleted, fc.handleGossipFetchStatus)
		gossip.AddMessageHandler(MsgTypeFetchFailed, fc.handleGossipFetchStatus)
	}

	return fc
}

// Fetch coordinates fetching a key, using deduplication and rate limiting
func (fc *FetchCoordinator) Fetch(ctx context.Context, key string) ([]byte, error) {
	// Check circuit breaker before starting anything
	if !fc.allowRequest() {
		fc.mu.Lock()
		fc.metrics.CircuitBreakerBlocks++
		fc.mu.Unlock()
		return nil, fmt.Errorf("s3 fetch blocked by circuit breaker (throttle protection active)")
	}

	startTime := time.Now()

	// Use singleflight to deduplicate concurrent requests on this node
	result, err, shared := fc.fetchGroup.Do(key, func() (any, error) {
		return fc.coordinatedFetch(ctx, key)
	})

	fc.mu.Lock()
	if shared {
		fc.metrics.CoalescedRequests++
	}
	waitTime := time.Since(startTime)
	fc.metrics.TotalWaitTime += waitTime
	fc.mu.Unlock()

	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, nil
	}
	return result.([]byte), nil
}

// allowRequest determines if the circuit breaker allows the fetch
func (fc *FetchCoordinator) allowRequest() bool {
	fc.circuitMu.RLock()
	state := fc.circuitState
	lastFail := fc.lastFailure
	fc.circuitMu.RUnlock()

	if state == StateClosed {
		return true
	}

	if state == StateOpen {
		// Check if break duration has passed
		if time.Since(lastFail) > fc.config.CircuitBreakDuration {
			fc.circuitMu.Lock()
			fc.circuitState = StateHalfOpen
			fc.circuitMu.Unlock()
			return true
		}
		return false
	}

	// In HalfOpen, allow limited requests (handled by the singleflight deduplicator effectively)
	return true
}

func (fc *FetchCoordinator) recordSuccess() {
	fc.circuitMu.Lock()
	defer fc.circuitMu.Unlock()
	fc.failureCount = 0
	fc.circuitState = StateClosed
}

func (fc *FetchCoordinator) recordFailure() {
	count := atomic.AddInt64(&fc.failureCount, 1)
	
	fc.circuitMu.Lock()
	defer fc.circuitMu.Unlock()
	fc.lastFailure = time.Now()
	
	if count >= fc.config.CircuitFailureThreshold {
		if fc.circuitState != StateOpen {
			fc.mu.Lock()
			fc.metrics.CircuitBreakerTrips++
			fc.mu.Unlock()
		}
		fc.circuitState = StateOpen
	}
}

// coordinatedFetch performs the actual fetch with distributed coordination
func (fc *FetchCoordinator) coordinatedFetch(ctx context.Context, key string) ([]byte, error) {
	if fc.config.EnableDistributedCoordination && fc.lockManager != nil {
		return fc.distributedFetch(ctx, key)
	}
	return fc.rateLimitedFetch(ctx, key)
}

// distributedFetch uses distributed locks to coordinate fetches across the cluster
func (fc *FetchCoordinator) distributedFetch(ctx context.Context, key string) ([]byte, error) {
	lockKey := "fetch:" + key
	acquired, err := fc.lockManager.TryLock(ctx, lockKey, LockTypeExclusive)
	if err != nil || !acquired {
		fc.mu.Lock()
		fc.metrics.CoordinatedRequests++
		fc.mu.Unlock()
		return fc.waitForFetch(ctx, key)
	}
	defer fc.lockManager.Unlock(ctx, lockKey)

	if fc.gossip != nil {
		fc.broadcastFetchStarted(key)
	}

	fc.trackFetchStarted(key)
	data, err := fc.rateLimitedFetch(ctx, key)

	if fc.gossip != nil {
		if err == nil {
			fc.broadcastFetchCompleted(key)
		} else {
			fc.broadcastFetchFailed(key, err)
		}
	}
	fc.trackFetchCompleted(key)

	return data, err
}

// rateLimitedFetch performs a rate-limited fetch with exponential backoff and jitter
func (fc *FetchCoordinator) rateLimitedFetch(ctx context.Context, key string) ([]byte, error) {
	if fc.rateLimiter != nil {
		if err := fc.rateLimiter.Wait(ctx); err != nil {
			fc.mu.Lock()
			fc.metrics.RateLimitedRequests++
			fc.mu.Unlock()
			return nil, fmt.Errorf("rate limit wait failed: %w", err)
		}
	}

	if fc.semaphore != nil {
		select {
		case fc.semaphore <- struct{}{}:
			defer func() { <-fc.semaphore }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	fetchCtx := ctx
	if fc.config.FetchTimeout > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(ctx, fc.config.FetchTimeout)
		defer cancel()
	}

	var data []byte
	var err error
	
	// Implementation of Exponential Backoff with Jitter (Full Jitter)
	for attempt := 0; attempt <= fc.config.MaxRetries; attempt++ {
		data, err = fc.fetchFunc(fetchCtx, key)
		if err == nil {
			fc.recordSuccess()
			break
		}

		// Record failure for circuit breaker
		fc.recordFailure()

		if attempt < fc.config.MaxRetries {
			// exponential backoff: 2^attempt * base
			expBackoff := fc.config.RetryBackoff * time.Duration(1<<uint(attempt))
			if expBackoff > fc.config.RetryMaxBackoff {
				expBackoff = fc.config.RetryMaxBackoff
			}
			
			// Full Jitter: rand(0, backoff)
			jitteredBackoff := time.Duration(rand.Int63n(int64(expBackoff)))

			select {
			case <-time.After(jitteredBackoff):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	fc.mu.Lock()
	if err == nil {
		fc.metrics.ObjectStoreFetches++
	} else {
		fc.metrics.FailedFetches++
		if ctx.Err() == context.DeadlineExceeded {
			fc.metrics.TimeoutFetches++
		}
	}
	fc.mu.Unlock()

	return data, err
}

// waitForFetch waits for another node to complete the fetch
func (fc *FetchCoordinator) waitForFetch(ctx context.Context, key string) ([]byte, error) {
	waitCtx := ctx
	if fc.config.QueueTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, fc.config.QueueTimeout)
		defer cancel()
	}

	fc.mu.Lock()
	fc.metrics.QueuedRequests++
	fc.mu.Unlock()

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fc.statusMu.RLock()
			_, fetching := fc.fetchStatus[key]
			fc.statusMu.RUnlock()

			if !fetching {
				// The other node finished. We now fetch from local storage 
				// (handled by the caller or a local check).
				return fc.rateLimitedFetch(ctx, key)
			}

		case <-waitCtx.Done():
			return fc.rateLimitedFetch(ctx, key)
		}
	}
}

func (fc *FetchCoordinator) trackFetchStarted(key string) {
	fc.statusMu.Lock()
	defer fc.statusMu.Unlock()
	fc.fetchStatus[key] = &FetchStatus{
		Key:       key,
		StartTime: time.Now(),
	}
}

func (fc *FetchCoordinator) trackFetchCompleted(key string) {
	fc.statusMu.Lock()
	defer fc.statusMu.Unlock()
	delete(fc.fetchStatus, key)
}

func (fc *FetchCoordinator) broadcastFetchStarted(key string) {
	if fc.gossip == nil { return }
	msg := &GossipMessage{
		Type: MsgTypeFetchStarted,
		Payload: &FetchStatusMessage{Key: key, Status: "started", Timestamp: time.Now()},
	}
	fc.gossip.Broadcast(msg)
}

func (fc *FetchCoordinator) broadcastFetchCompleted(key string) {
	if fc.gossip == nil { return }
	msg := &GossipMessage{
		Type: MsgTypeFetchCompleted,
		Payload: &FetchStatusMessage{Key: key, Status: "completed", Timestamp: time.Now()},
	}
	fc.gossip.Broadcast(msg)
}

func (fc *FetchCoordinator) broadcastFetchFailed(key string, err error) {
	if fc.gossip == nil { return }
	msg := &GossipMessage{
		Type: MsgTypeFetchFailed,
		Payload: &FetchStatusMessage{Key: key, Status: "failed", Timestamp: time.Now(), Error: err.Error()},
	}
	fc.gossip.Broadcast(msg)
}

func (fc *FetchCoordinator) GetMetrics() FetchMetrics {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.metrics
}

type FetchStatusMessage struct {
	Key       string
	Status    string
	Timestamp time.Time
	Error     string
}

func (fc *FetchCoordinator) handleFetchStatusMessage(msg *FetchStatusMessage) {
	fc.statusMu.Lock()
	defer fc.statusMu.Unlock()
	switch msg.Status {
	case "started":
		if _, exists := fc.fetchStatus[msg.Key]; !exists {
			fc.fetchStatus[msg.Key] = &FetchStatus{Key: msg.Key, StartTime: msg.Timestamp}
		}
	case "completed", "failed":
		delete(fc.fetchStatus, msg.Key)
	}
}

func (fc *FetchCoordinator) handleGossipFetchStatus(msg *GossipMessage) {
	if fetchStatusMsg, ok := msg.Payload.(*FetchStatusMessage); ok {
		fc.handleFetchStatusMessage(fetchStatusMsg)
	}
}
