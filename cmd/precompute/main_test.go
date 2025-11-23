package main

import (
	"testing"
	"time"
)

func TestFormatElapsed(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "zero duration",
			duration: 0,
			want:     "0s",
		},
		{
			name:     "one second",
			duration: 1 * time.Second,
			want:     "1s",
		},
		{
			name:     "multiple seconds",
			duration: 45 * time.Second,
			want:     "45s",
		},
		{
			name:     "59 seconds",
			duration: 59 * time.Second,
			want:     "59s",
		},
		{
			name:     "exactly 60 seconds",
			duration: 60 * time.Second,
			want:     "1m00s",
		},
		{
			name:     "one minute",
			duration: 1 * time.Minute,
			want:     "1m00s",
		},
		{
			name:     "one minute and 30 seconds",
			duration: 90 * time.Second,
			want:     "1m30s",
		},
		{
			name:     "multiple minutes",
			duration: 5 * time.Minute,
			want:     "5m00s",
		},
		{
			name:     "multiple minutes and seconds",
			duration: 5*time.Minute + 42*time.Second,
			want:     "5m42s",
		},
		{
			name:     "10 minutes",
			duration: 10 * time.Minute,
			want:     "10m00s",
		},
		{
			name:     "large duration - 60 minutes",
			duration: 60 * time.Minute,
			want:     "60m00s",
		},
		{
			name:     "large duration - 120 minutes",
			duration: 120 * time.Minute,
			want:     "120m00s",
		},
		{
			name:     "with milliseconds (should round)",
			duration: 5*time.Second + 500*time.Millisecond,
			want:     "6s",
		},
		{
			name:     "with milliseconds less than 500 (should round down)",
			duration: 5*time.Second + 400*time.Millisecond,
			want:     "5s",
		},
		{
			name:     "minutes with milliseconds",
			duration: 2*time.Minute + 30*time.Second + 600*time.Millisecond,
			want:     "2m31s",
		},
		{
			name:     "very small duration (milliseconds only)",
			duration: 100 * time.Millisecond,
			want:     "0s",
		},
		{
			name:     "almost one second",
			duration: 999 * time.Millisecond,
			want:     "1s",
		},
		{
			name:     "59 minutes 59 seconds",
			duration: 59*time.Minute + 59*time.Second,
			want:     "59m59s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatElapsed(tt.duration)
			if got != tt.want {
				t.Errorf("formatElapsed(%v) = %q, want %q", tt.duration, got, tt.want)
			}
		})
	}
}

func TestFormatElapsed_Consistency(t *testing.T) {
	// Test that same duration always produces same result
	duration := 3*time.Minute + 42*time.Second

	result1 := formatElapsed(duration)
	result2 := formatElapsed(duration)

	if result1 != result2 {
		t.Errorf("formatElapsed not consistent: got %q and %q for same duration", result1, result2)
	}
}

func TestFormatElapsed_NegativeDuration(t *testing.T) {
	// Go's time.Duration can be negative, test handling
	// Note: Round() on negative durations still works
	duration := -5 * time.Second

	// The function should still work (may produce negative values or 0)
	result := formatElapsed(duration)

	// Just verify it doesn't panic and returns something
	if result == "" {
		t.Error("formatElapsed returned empty string for negative duration")
	}
}

// Benchmarks

func BenchmarkFormatElapsed_Seconds(b *testing.B) {
	duration := 45 * time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatElapsed(duration)
	}
}

func BenchmarkFormatElapsed_Minutes(b *testing.B) {
	duration := 5*time.Minute + 42*time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatElapsed(duration)
	}
}

func BenchmarkFormatElapsed_LargeDuration(b *testing.B) {
	duration := 120*time.Minute + 33*time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatElapsed(duration)
	}
}
