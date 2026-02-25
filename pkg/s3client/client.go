package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client wraps the AWS S3 client
type Client struct {
	s3Client   *s3.Client
	timeout    time.Duration
	maxRetries int
}

// ClientOption is a functional option for Client
type ClientOption func(*Client)

// WithTimeout sets the request timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithMaxRetries sets the maximum number of retries
func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

// New creates a new S3 client
func New(endpoint, region, accessKeyID, secretAccessKey string, opts ...ClientOption) (*Client, error) {
	// Create AWS config
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client options
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = region
		},
	}

	// Add custom endpoint if provided
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg, s3Opts...)

	client := &Client{
		s3Client:   s3Client,
		timeout:    30 * time.Second,
		maxRetries: 3,
	}

	// Apply options
	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// GetObject retrieves an object from S3
func (c *Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Get object
	result, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	// Read body
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return data, nil
}

// PutObject uploads an object to S3
func (c *Client) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Put object
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

// DeleteObject deletes an object from S3
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Delete object
	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// ListObjects lists all objects in a bucket with an optional prefix.
// FIX #31: the previous implementation issued a single ListObjectsV2 call
// which silently truncated results at 1000 keys (the S3 API page size).
// This version paginates using ContinuationToken until IsTruncated is false.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	// Use a per-call timeout that is generous enough for large buckets.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(1000), // explicit page size
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	var keys []string

	for {
		result, err := c.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		// If the result is not truncated we have fetched all pages.
		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}

		// Advance to the next page using the continuation token.
		input.ContinuationToken = result.NextContinuationToken
	}

	return keys, nil
}

// HeadObject retrieves object metadata
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Head object
	result, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head object: %w", err)
	}

	return result, nil
}

// GetObjectRange retrieves a range of bytes from an object
func (c *Client) GetObjectRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Get object with range
	rangeStr := fmt.Sprintf("bytes=%d-%d", start, end)
	result, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object range: %w", err)
	}
	defer result.Body.Close()

	// Read body
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return data, nil
}