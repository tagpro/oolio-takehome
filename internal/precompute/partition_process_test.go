package precompute

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessBucket_Scenarios tests processBucket with various scenarios
func TestProcessBucket_Scenarios(t *testing.T) {
	tests := []struct {
		name          string
		content       string
		expectedCodes []string
		expectedCount int
	}{
		{
			name: "SingleFileOnly",
			content: `CODE1|0
CODE2|1
CODE3|2
CODE4|0
CODE5|1`,
			expectedCodes: []string{},
			expectedCount: 0,
		},
		{
			name: "DuplicateEntries",
			content: `CODE1|0
CODE1|1
CODE1|0
CODE1|1
CODE1|2`,
			expectedCodes: []string{"CODE1"},
			expectedCount: 1,
		},
		{
			name: "MixedValidInvalid",
			content: `VALID1|0
VALID1|1
INVALID1|0
VALID2|0
VALID2|1
VALID2|2
INVALID2|1
VALID3|0
VALID3|1`,
			expectedCodes: []string{"VALID1", "VALID2", "VALID3"},
			expectedCount: 3,
		},
		{
			name: "OrderIndependence_1",
			content: `CODE1|0
CODE1|1
CODE2|0
CODE2|1
CODE3|0
CODE3|1`,
			expectedCodes: []string{"CODE1", "CODE2", "CODE3"},
			expectedCount: 3,
		},
		{
			name: "OrderIndependence_2",
			content: `CODE3|1
CODE3|0
CODE1|1
CODE1|0
CODE2|1
CODE2|0`,
			expectedCodes: []string{"CODE1", "CODE2", "CODE3"},
			expectedCount: 3,
		},
		{
			name: "Malformed_MissingPipe",
			content: `TESTCODE0
GOODCODE|1
GOODCODE|2`,
			expectedCodes: []string{"GOODCODE"},
			expectedCount: 1,
		},
		{
			name: "Malformed_MultiplePipes",
			content: `TESTCODE|0|extra
GOODCODE|1
GOODCODE|2`,
			expectedCodes: []string{"GOODCODE"},
			expectedCount: 1,
		},
		{
			name: "Malformed_NonNumericIndex",
			content: `TESTCODE|abc
GOODCODE|1
GOODCODE|2`,
			expectedCodes: []string{"GOODCODE"},
			expectedCount: 1,
		},
		{
			name: "Malformed_NegativeIndex",
			content: `TESTCODE|-1
GOODCODE|0
GOODCODE|1`,
			expectedCodes: []string{"GOODCODE"},
			expectedCount: 1,
		},
		{
			name: "Malformed_EmptyLines",
			content: `

GOODCODE|0

GOODCODE|1

`,
			expectedCodes: []string{"GOODCODE"},
			expectedCount: 1,
		},
		{
			name: "Malformed_WhitespaceInCode",
			content: `GOOD CODE|0
GOOD CODE|1`,
			expectedCodes: []string{"GOOD CODE"},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			bucketPath := filepath.Join(tmpDir, "bucket.txt")
			err := os.WriteFile(bucketPath, []byte(tt.content), 0644)
			require.NoError(t, err, "Failed to create test bucket file")

			validCodes, err := processBucket(bucketPath)
			require.NoError(t, err, "processBucket should not return error")

			sort.Strings(validCodes)
			sort.Strings(tt.expectedCodes)

			if tt.expectedCount > 0 {
				require.Len(t, validCodes, tt.expectedCount)
				assert.Equal(t, tt.expectedCodes, validCodes)
			} else {
				assert.Empty(t, validCodes)
			}
		})
	}
}

// TestProcessBucket_LargeDataset tests processing a large bucket
func TestProcessBucket_LargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// Create a large bucket with 1,000 unique codes, each in 3 files
	numCodes := 1000
	content := ""
	for i := 0; i < numCodes; i++ {
		// Generate unique codes using multiple digits
		code := "LARGE" + string(rune('A'+(i/100)%26)) + string(rune('A'+(i/10)%26)) + string(rune('A'+i%26))
		content += code + "|0\n"
		content += code + "|1\n"
		content += code + "|2\n"
	}

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	require.NoError(t, err, "Failed to create test bucket file")

	validCodes, err := processBucket(bucketPath)
	require.NoError(t, err, "processBucket should not return error")

	// All 1,000 codes should be valid
	assert.Len(t, validCodes, numCodes, "Expected %d valid codes", numCodes)
}

// TestProcessBucketsWorker tests the worker function with various scenarios
func TestProcessBucketsWorker(t *testing.T) {
	tests := []struct {
		name           string
		buckets        []string // content of each bucket
		numWorkers     int
		expectedCodes  []string
		expectedError  bool
		invalidBuckets []string // paths to non-existent buckets to inject
	}{
		{
			name: "BasicWorker",
			buckets: []string{
				`CODE1|0
CODE1|1
CODE2|0
CODE2|1`,
				`CODE3|0
CODE3|1
CODE4|0`,
				`CODE5|0
CODE5|1
CODE5|2`,
			},
			numWorkers:    1,
			expectedCodes: []string{"CODE1", "CODE2", "CODE3", "CODE5"},
			expectedError: false,
		},
		{
			name:          "EmptyChannel",
			buckets:       []string{},
			numWorkers:    1,
			expectedCodes: []string{},
			expectedError: false,
		},
		{
			name: "ErrorHandling",
			buckets: []string{
				`CODE1|0
CODE1|1`,
			},
			numWorkers:     1,
			expectedCodes:  nil,
			expectedError:  true,
			invalidBuckets: []string{"nonexistent_bucket.txt"},
		},
		{
			name: "MultipleWorkers",
			buckets: func() []string {
				var b []string
				for i := 0; i < 10; i++ {
					content := "CODE" + string(rune('A'+i)) + "|0\n" +
						"CODE" + string(rune('A'+i)) + "|1\n"
					b = append(b, content)
				}
				return b
			}(),
			numWorkers: 3,
			expectedCodes: func() []string {
				var c []string
				for i := 0; i < 10; i++ {
					c = append(c, "CODE"+string(rune('A'+i)))
				}
				return c
			}(),
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			bucketPaths := make(chan string, len(tt.buckets)+len(tt.invalidBuckets))
			results := make(chan []string, len(tt.buckets)+len(tt.invalidBuckets))

			// Create bucket files
			for i, content := range tt.buckets {
				bucketPath := filepath.Join(tmpDir, fmt.Sprintf("bucket_%d.txt", i))
				err := os.WriteFile(bucketPath, []byte(content), 0644)
				require.NoError(t, err)
				bucketPaths <- bucketPath
			}

			// Add invalid buckets
			for _, path := range tt.invalidBuckets {
				bucketPaths <- filepath.Join(tmpDir, path)
			}
			close(bucketPaths)

			// Run workers
			errors := make(chan error, tt.numWorkers)
			for w := 0; w < tt.numWorkers; w++ {
				workerID := w
				go func() {
					errors <- processBucketsWorker(workerID, bucketPaths, results)
				}()
			}

			// Wait for workers and check errors
			var gotError bool
			for w := 0; w < tt.numWorkers; w++ {
				err := <-errors
				if err != nil {
					gotError = true
				}
			}

			if tt.expectedError {
				assert.True(t, gotError, "Expected error but got none")
				return
			}
			require.False(t, gotError, "Worker returned unexpected error")
			close(results)

			// Collect results
			allCodes := []string{}
			for codes := range results {
				allCodes = append(allCodes, codes...)
			}

			sort.Strings(allCodes)
			sort.Strings(tt.expectedCodes)

			assert.Equal(t, tt.expectedCodes, allCodes)
		})
	}
}

// Benchmarks

func BenchmarkProcessBucketsWorker_SingleWorker(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 10 small bucket files
	numBuckets := 10
	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+i))+".txt")
		content := ""
		for j := 0; j < 50; j++ {
			code := "CODE" + string(rune('A'+j%26))
			content += code + "|0\n"
			content += code + "|1\n"
		}
		err := os.WriteFile(bucketPath, []byte(content), 0644)
		if err != nil {
			b.Fatalf("Failed to create bucket: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucketPaths := make(chan string, numBuckets)
		results := make(chan []string, numBuckets)

		for j := 0; j < numBuckets; j++ {
			bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+j))+".txt")
			bucketPaths <- bucketPath
		}
		close(bucketPaths)

		err := processBucketsWorker(1, bucketPaths, results)
		if err != nil {
			b.Fatalf("processBucketsWorker() error = %v", err)
		}
		close(results)

		// Drain results
		for range results {
		}
	}
}

func BenchmarkProcessBucketsWorker_MultipleWorkers(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 100 bucket files
	numBuckets := 100
	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+(i%10)))+string(rune('0'+(i/10)%10))+".txt")
		content := ""
		for j := 0; j < 50; j++ {
			code := "CODE" + string(rune('A'+j%26)) + string(rune('0'+i%10))
			content += code + "|0\n"
			content += code + "|1\n"
		}
		err := os.WriteFile(bucketPath, []byte(content), 0644)
		if err != nil {
			b.Fatalf("Failed to create bucket: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucketPaths := make(chan string, numBuckets)
		results := make(chan []string, numBuckets)

		// Fill bucket paths
		for j := 0; j < numBuckets; j++ {
			bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+(j%10)))+string(rune('0'+(j/10)%10))+".txt")
			bucketPaths <- bucketPath
		}
		close(bucketPaths)

		// Run 4 workers
		numWorkers := 4
		errors := make(chan error, numWorkers)
		for w := 0; w < numWorkers; w++ {
			workerID := w
			go func() {
				errors <- processBucketsWorker(workerID, bucketPaths, results)
			}()
		}

		// Wait for workers
		for w := 0; w < numWorkers; w++ {
			if err := <-errors; err != nil {
				b.Fatalf("Worker error: %v", err)
			}
		}
		close(results)

		// Drain results
		for range results {
		}
	}
}
