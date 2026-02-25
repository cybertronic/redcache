package proxy

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

// Server represents the S3-compatible proxy server
type Server struct {
	router         *mux.Router
	httpServer     *http.Server
	storage        StorageInterface
	s3Client       S3ClientInterface
	metrics        MetricsInterface
	readTimeout    time.Duration
	writeTimeout   time.Duration
	maxRequestSize int64
}

// StorageInterface defines cache storage operations
type StorageInterface interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// S3ClientInterface defines S3 operations
type S3ClientInterface interface {
	GetObject(ctx context.Context, bucket, key string) ([]byte, error)
	PutObject(ctx context.Context, bucket, key string, data []byte) error
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string) ([]string, error)
	GetObjectRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// MetricsInterface defines metrics operations
type MetricsInterface interface {
	RecordCacheHit()
	RecordCacheMiss()
	RecordRequest(duration time.Duration, requestSize, responseSize int64)
	RecordS3Request(duration time.Duration, err error)
}

// ServerOption is a functional option for Server
type ServerOption func(*Server)

// WithMetrics sets the metrics collector
func WithMetrics(metrics MetricsInterface) ServerOption {
	return func(s *Server) {
		s.metrics = metrics
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.readTimeout = timeout
	}
}

// WithWriteTimeout sets the write timeout
func WithWriteTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.writeTimeout = timeout
	}
}

// WithMaxRequestSize sets the maximum request size
func WithMaxRequestSize(size int64) ServerOption {
	return func(s *Server) {
		s.maxRequestSize = size
	}
}

// NewServer creates a new proxy server
func NewServer(storage StorageInterface, s3Client S3ClientInterface, addr string, opts ...ServerOption) *Server {
	s := &Server{
		router:         mux.NewRouter(),
		storage:        storage,
		s3Client:       s3Client,
		readTimeout:    60 * time.Second,
		writeTimeout:   60 * time.Second,
		maxRequestSize: 100 * 1024 * 1024, // 100MB
	}

	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	// Setup routes
	s.setupRoutes()

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
	}

	return s
}

// setupRoutes configures the HTTP routes
func (s *Server) setupRoutes() {
	// S3-compatible API routes
	s.router.HandleFunc("/{bucket}/{key:.*}", s.handleGetObject).Methods("GET", "HEAD")
	s.router.HandleFunc("/{bucket}/{key:.*}", s.handlePutObject).Methods("PUT")
	s.router.HandleFunc("/{bucket}/{key:.*}", s.handleDeleteObject).Methods("DELETE")
	s.router.HandleFunc("/{bucket}", s.handleListObjects).Methods("GET")
	s.router.HandleFunc("/{bucket}/", s.handleListObjects).Methods("GET")

	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
}

// handleGetObject handles GET/HEAD requests for objects
func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	cacheKey := fmt.Sprintf("%s/%s", bucket, key)

	// Check for range request
	rangeHeader := r.Header.Get("Range")
	hasRange := rangeHeader != ""

	// Try cache first (only for non-range requests)
	if !hasRange {
		data, found := s.storage.Get(r.Context(), cacheKey)
		if found {
			log.Debug().Str("key", cacheKey).Msg("Cache hit")
			if s.metrics != nil {
				s.metrics.RecordCacheHit()
			}

			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Content-Type", "application/octet-stream")

			if r.Method == "HEAD" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write(data)
			}

			if s.metrics != nil {
				s.metrics.RecordRequest(time.Since(startTime), 0, int64(len(data)))
			}
			return
		}

		if s.metrics != nil {
			s.metrics.RecordCacheMiss()
		}
	}

	// Cache miss or range request - fetch from S3
	log.Debug().Str("key", cacheKey).Bool("range", hasRange).Msg("Cache miss, fetching from S3")

	var data []byte
	var err error

	if hasRange {
		// Parse range header
		start, end, parseErr := parseRangeHeader(rangeHeader)
		if parseErr != nil {
			http.Error(w, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		s3Start := time.Now()
		data, err = s.s3Client.GetObjectRange(r.Context(), bucket, key, start, end)
		if s.metrics != nil {
			s.metrics.RecordS3Request(time.Since(s3Start), err)
		}
		if err == nil {
			// FIX #19: Content-Range must be "bytes start-end/total" per RFC 7233.
			// We use the actual returned data length as the total since we may not
			// know the full object size without a HEAD request.
			total := start + int64(len(data))
			actualEnd := start + int64(len(data)) - 1
			if actualEnd < start {
				actualEnd = start
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, actualEnd, total))
		}
	} else {
		s3Start := time.Now()
		data, err = s.s3Client.GetObject(r.Context(), bucket, key)
		if s.metrics != nil {
			s.metrics.RecordS3Request(time.Since(s3Start), err)
		}
	}

	if err != nil {
		log.Error().Err(err).Str("key", cacheKey).Msg("Failed to fetch from S3")
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}

	// Store in cache (only for non-range requests)
	if !hasRange {
		if err := s.storage.Put(r.Context(), cacheKey, data); err != nil {
			log.Warn().Err(err).Str("key", cacheKey).Msg("Failed to cache object")
		}
	}

	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Type", "application/octet-stream")

	if hasRange {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if r.Method != "HEAD" {
		w.Write(data)
	}

	if s.metrics != nil {
		s.metrics.RecordRequest(time.Since(startTime), 0, int64(len(data)))
	}
}

// handlePutObject handles PUT requests for objects
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	cacheKey := fmt.Sprintf("%s/%s", bucket, key)

	// FIX #21: reject requests whose Content-Length exceeds the limit AND
	// also reject chunked-encoding requests that exceed the limit after reading.
	contentLength := r.ContentLength
	if contentLength > s.maxRequestSize {
		http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Read body with a hard cap regardless of Content-Length (-1 = chunked).
	// io.LimitReader caps at maxRequestSize+1 so we can detect overflow.
	limitReader := io.LimitReader(r.Body, s.maxRequestSize+1)
	data, err := io.ReadAll(limitReader)
	if err != nil {
		log.Error().Err(err).Msg("Failed to read request body")
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// FIX #21: if we read more than maxRequestSize bytes the request is too large.
	if int64(len(data)) > s.maxRequestSize {
		http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Put to S3
	s3Start := time.Now()
	err = s.s3Client.PutObject(r.Context(), bucket, key, data)
	if s.metrics != nil {
		s.metrics.RecordS3Request(time.Since(s3Start), err)
	}

	if err != nil {
		log.Error().Err(err).Str("key", cacheKey).Msg("Failed to put to S3")
		http.Error(w, "Failed to store object", http.StatusInternalServerError)
		return
	}

	// Update cache
	if err := s.storage.Put(r.Context(), cacheKey, data); err != nil {
		log.Warn().Err(err).Str("key", cacheKey).Msg("Failed to cache object")
	}

	// FIX #5: ETag must be a properly quoted string, not HTML-entity-encoded.
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, len(data)))
	w.WriteHeader(http.StatusOK)

	if s.metrics != nil {
		s.metrics.RecordRequest(time.Since(startTime), int64(len(data)), 0)
	}
}

// handleDeleteObject handles DELETE requests for objects
func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	cacheKey := fmt.Sprintf("%s/%s", bucket, key)

	// Delete from S3
	s3Start := time.Now()
	err := s.s3Client.DeleteObject(r.Context(), bucket, key)
	if s.metrics != nil {
		s.metrics.RecordS3Request(time.Since(s3Start), err)
	}

	if err != nil {
		log.Error().Err(err).Str("key", cacheKey).Msg("Failed to delete from S3")
		http.Error(w, "Failed to delete object", http.StatusInternalServerError)
		return
	}

	// Delete from cache
	if err := s.storage.Delete(r.Context(), cacheKey); err != nil {
		log.Warn().Err(err).Str("key", cacheKey).Msg("Failed to delete from cache")
	}

	w.WriteHeader(http.StatusNoContent)

	if s.metrics != nil {
		s.metrics.RecordRequest(time.Since(startTime), 0, 0)
	}
}

// handleListObjects handles LIST requests for buckets
func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	prefix := r.URL.Query().Get("prefix")

	s3Start := time.Now()
	objects, err := s.s3Client.ListObjects(r.Context(), bucket, prefix)
	if s.metrics != nil {
		s.metrics.RecordS3Request(time.Since(s3Start), err)
	}

	if err != nil {
		log.Error().Err(err).Str("bucket", bucket).Msg("Failed to list objects")
		http.Error(w, "Failed to list objects", http.StatusInternalServerError)
		return
	}

	// FIX #20: build XML response using encoding/xml to prevent injection.
	xmlBytes, err := buildListObjectsResponse(bucket, prefix, objects)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build list objects response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write(xmlBytes)

	if s.metrics != nil {
		s.metrics.RecordRequest(time.Since(startTime), 0, int64(len(xmlBytes)))
	}
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Start starts the proxy server
func (s *Server) Start() error {
	log.Info().Str("addr", s.httpServer.Addr).Msg("Starting proxy server")
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the proxy server
func (s *Server) Shutdown(ctx context.Context) error {
	log.Info().Msg("Stopping proxy server")
	return s.httpServer.Shutdown(ctx)
}

// Helper functions

// parseRangeHeader parses HTTP Range header.
// Returns an error if the header is malformed.
func parseRangeHeader(rangeHeader string) (start, end int64, err error) {
	// Format: "bytes=start-end"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range unit in: %s", rangeHeader)
	}
	rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeHeader, "-")

	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	if parts[0] != "" {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range start: %w", err)
		}
	}
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range end: %w", err)
		}
	}

	return start, end, nil
}

// listBucketResult is the top-level XML structure for ListBucketResult.
// FIX #20: using encoding/xml ensures all string fields are properly escaped.
type listBucketResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Xmlns       string         `xml:"xmlns,attr"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []s3ObjectItem `xml:"Contents"`
}

type s3ObjectItem struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// buildListObjectsResponse builds an S3-compatible XML response.
// FIX #20: uses encoding/xml.Marshal so object keys are properly escaped,
// preventing XML injection via malicious key names.
func buildListObjectsResponse(bucket, prefix string, keys []string) ([]byte, error) {
	result := listBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		MaxKeys:     1000,
		IsTruncated: false,
		Contents:    make([]s3ObjectItem, 0, len(keys)),
	}

	now := time.Now().Format(time.RFC3339)
	for _, key := range keys {
		result.Contents = append(result.Contents, s3ObjectItem{
			Key:          key, // xml.Marshal will escape special characters
			LastModified: now,
			Size:         0,
			StorageClass: "STANDARD",
		})
	}

	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	enc := xml.NewEncoder(&buf)
	enc.Indent("", "    ")
	if err := enc.Encode(result); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}