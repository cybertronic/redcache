package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"redcache/pkg/cache"
	"redcache/pkg/config"
	"redcache/pkg/distributed"
	"redcache/pkg/erasure"
	"redcache/pkg/iouring"
	"redcache/pkg/metrics"
	"redcache/pkg/proxy"
	"redcache/pkg/s3client"
	"redcache/pkg/storage"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	configFile = flag.String("config", "/etc/redcache/config.yaml", "Configuration file path")
	version    = "1.2.0-stable"
	commit     = "unknown"
	buildTime  = "unknown"
)

func main() {
	flag.Parse()

	// 0. Setup structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	log.Info().
		Str("version", version).
		Str("commit", commit).
		Str("buildTime", buildTime).
		Msg("Starting RedCache Agent")

	// 1. Load & Validate Configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// 2. Initialize io_uring Subsystem
	iouringConfig := iouring.DefaultConfig()
	iouringConfig.Enabled = cfg.ZeroCopy.IOUring.Enabled
	iouringConfig.QueueDepth = cfg.ZeroCopy.IOUring.QueueDepth
	iouringConfig.NumFixedBuffers = cfg.ZeroCopy.IOUring.NumFixedBuffers
	iouringConfig.FixedBufferSize = cfg.ZeroCopy.IOUring.FixedBufferSize
	
	iouringMgr, err := iouring.NewManager(iouringConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize io_uring")
	}
	defer iouringMgr.Close()

	// 3. Initialize Persistent Storage (BadgerDB)
	dbConfig := storage.DefaultDatabaseConfig(cfg.Storage.Database.Path)
	db, err := storage.NewDatabase(dbConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize BadgerDB")
	}
	defer db.Close()

	// 4. Initialize Local Cache Storage
	cacheStorage, err := cache.NewStorage(cfg.Cache.Directory, cfg.Cache.MaxSize)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize cache storage")
	}
	defer cacheStorage.Close()

	// 5. Initialize S3 Backend Client
	s3Client, err := s3client.New(
		cfg.S3.Endpoint,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
		s3client.WithTimeout(cfg.S3.Timeout),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize S3 client")
	}

	// 6. Initialize Distributed Architecture
	clusterConfig := &distributed.ClusterConfig{
		NodeID:       cfg.Distributed.NodeID,
		BindAddr:     cfg.Distributed.BindAddr,
		BindPort:     cfg.Distributed.BindPort,
		RPCPort:      cfg.Distributed.RPCPort,
		Seeds:        cfg.Distributed.Seeds,
		EnableQuorum: cfg.Distributed.EnableQuorum,
	}

	fetchConfig := distributed.FetchCoordinatorConfig{
		EnableDistributedCoordination: cfg.Distributed.EnableQuorum,
		RequestsPerSecond:             100,
		MaxConcurrentFetches:          20,
		FetchTimeout:                  cfg.S3.Timeout,
		MaxRetries:                    cfg.S3.MaxRetries,
		RetryBackoff:                  cfg.S3.RetryBackoff,
		RetryMaxBackoff:               cfg.S3.RetryMaxBackoff,
		CircuitFailureThreshold:       cfg.S3.CircuitFailureThreshold,
		CircuitBreakDuration:          cfg.S3.CircuitBreakDuration,
	}

	distCache, err := distributed.NewDistributedCache(clusterConfig, cacheStorage, s3Client, fetchConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Distributed Cache")
	}

	// 7. Initialize Erasure Coding & Subsystems
	encConfig := erasure.EncoderConfig{
		DataShards:   6,
		ParityShards: 3,
		ShardSize:    int64(cfg.ZeroCopy.IOUring.FixedBufferSize),
	}
	encoder, err := erasure.NewEncoder(encConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Erasure Encoder")
	}

	fragMgr := erasure.NewFragmentManager(encoder, distCache.ClusterManager(), distCache.RPCClient(), db)
	distCache.SetErasureCache(fragMgr)

	// 8. Start Distributed Mesh
	if err := distCache.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start Distributed Cache mesh")
	}

	// 9. Initialize Proxy Layer
	metricsCollector := metrics.NewCollector(cfg.Metrics.Namespace)
	proxyServer := proxy.NewServer(cacheStorage, s3Client, cfg.Proxy.ListenAddr, proxy.WithMetrics(metricsCollector))

	go func() {
		log.Info().Str("addr", cfg.Proxy.ListenAddr).Msg("S3 Proxy Server Active")
		if err := proxyServer.Start(); err != nil {
			log.Fatal().Err(err).Msg("Proxy server failure")
		}
	}()

	// 10. Lifecycle Management
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Info().Msg("Shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	
	_ = distCache.Shutdown(shutdownCtx)
	_ = proxyServer.Shutdown(shutdownCtx)
	
	log.Info().Msg("RedCache Agent shutdown complete")
}
