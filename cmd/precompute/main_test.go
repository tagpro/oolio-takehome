package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			name:     "29 minutes 59 seconds",
			duration: 29*time.Minute + 59*time.Second,
			want:     "29m59s",
		},
		{
			name:     "159 minutes 59 seconds",
			duration: 159*time.Minute + 59*time.Second,
			want:     "159m59s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatElapsed(tt.duration)
			assert.Equal(t, tt.want, got, "formatElapsed should return expected format for %v", tt.duration)
		})
	}
}
