//go:build linux

package iouring

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectFeatures(t *testing.T) {
	f := DetectFeatures()
	require.NotNil(t, f)

	t.Logf("Detected kernel features: %s", f)

	// We're running on kernel 6.1 in this environment — all features should be present
	assert.True(t, f.Available, "io_uring should be available on kernel 6.1")
	assert.True(t, f.FixedBuffers, "fixed buffers should be available on kernel 6.1")
	assert.True(t, f.NetworkSupport, "network support should be available on kernel 6.1")
	assert.True(t, f.LinkedOps, "linked ops should be available on kernel 6.1")
	assert.True(t, f.TimeoutSupport, "timeout support should be available on kernel 6.1")
	assert.True(t, f.SendZCSupport, "SEND_ZC should be available on kernel 6.1")
	assert.True(t, f.BatchCQE, "batch CQE should be available on kernel 6.1")

	assert.Equal(t, 6, f.Major)
	assert.GreaterOrEqual(t, f.Minor, 0)
}

func TestDetectFeaturesCached(t *testing.T) {
	// DetectFeatures should return the same pointer on repeated calls
	f1 := DetectFeatures()
	f2 := DetectFeatures()
	assert.Same(t, f1, f2, "DetectFeatures should return cached result")
}

func TestParseKernelVersion(t *testing.T) {
	tests := []struct {
		input        string
		wantMajor    int
		wantMinor    int
		wantErr      bool
	}{
		{"6.1.155", 6, 1, false},
		{"5.15.0-generic", 5, 15, false},
		{"5.4.0-150-generic", 5, 4, false},
		{"6.6.0+", 6, 6, false},
		{"4.19.0", 4, 19, false},
		{"", 0, 0, true},
		{"abc", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			major, minor, err := parseKernelVersion(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMajor, major)
			assert.Equal(t, tt.wantMinor, minor)
		})
	}
}

func TestAtLeast(t *testing.T) {
	tests := []struct {
		major, minor, wantMajor, wantMinor int
		want                               bool
	}{
		{6, 1, 5, 1, true},
		{6, 1, 6, 0, true},
		{6, 1, 6, 1, true},
		{6, 1, 6, 2, false},
		{5, 15, 6, 0, false},
		{5, 7, 5, 7, true},
		{5, 6, 5, 7, false},
	}

	for _, tt := range tests {
		got := atLeast(tt.major, tt.minor, tt.wantMajor, tt.wantMinor)
		assert.Equal(t, tt.want, got,
			"atLeast(%d,%d, %d,%d)", tt.major, tt.minor, tt.wantMajor, tt.wantMinor)
	}
}

func TestKernelFeaturesString(t *testing.T) {
	f := &KernelFeatures{
		Major:          6,
		Minor:          1,
		Available:      true,
		NetworkSupport: true,
		SendZCSupport:  true,
		FixedBuffers:   true,
		LinkedOps:      true,
		SQPollSupport:  true,
	}
	s := f.String()
	assert.Contains(t, s, "kernel=6.1")
	assert.Contains(t, s, "available=true")
	assert.Contains(t, s, "sendZC=true")
}