package precompute

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHashCode verifies the hash function distributes codes evenly
func TestHashCode(t *testing.T) {
	t.Parallel() // Tests are independent, run in parallel

	tests := []struct {
		name       string
		code       string
		numBuckets int
	}{
		{
			name:       "normal code",
			code:       "HAPPYHRS",
			numBuckets: 10,
		},
		{
			name:       "empty string",
			code:       "",
			numBuckets: 10,
		},
		{
			name:       "very long code",
			code:       string(make([]byte, 10000)),
			numBuckets: 100,
		},
		{
			name:       "special characters",
			code:       "CODE@#$%",
			numBuckets: 10,
		},
		{
			name:       "unicode characters",
			code:       "CODE世界",
			numBuckets: 10,
		},
		{
			name:       "single bucket",
			code:       "TEST",
			numBuckets: 1,
		},
		{
			name:       "many buckets",
			code:       "TEST",
			numBuckets: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := hashCode(tt.code, tt.numBuckets)
			assert.GreaterOrEqual(t, bucket, 0, "Bucket should be >= 0")
			assert.Less(t, bucket, tt.numBuckets, "Bucket should be < numBuckets")

			// Verify deterministic
			bucket2 := hashCode(tt.code, tt.numBuckets)
			assert.Equal(t, bucket, bucket2, "hashCode should be deterministic")
		})
	}
}

func TestHashCode_Distribution(t *testing.T) {
	t.Parallel()

	numBuckets := 10
	testCodes := []string{
		"HAPPYHRS", "FIFTYOFF", "SUPER100", "TESTCODE",
		"ABCD1234", "WXYZ9876", "PROMO123", "DEAL5678",
		"SAVE20PC", "GETFREE1",
	}

	// Verify all codes hash to valid bucket numbers
	bucketCounts := make(map[int]int)
	for _, code := range testCodes {
		bucket := hashCode(code, numBuckets)
		assert.GreaterOrEqual(t, bucket, 0, "Bucket should be >= 0")
		assert.Less(t, bucket, numBuckets, "Bucket should be < numBuckets")
		bucketCounts[bucket]++
	}

	// Verify we get some distribution (not all in one bucket)
	assert.GreaterOrEqual(t, len(bucketCounts), 2, "Codes should distribute across multiple buckets")
}

// TestFindValidCodesHashPartition_Scenarios tests the full hash partition algorithm with various scenarios
func TestFindValidCodesHashPartition_Scenarios(t *testing.T) {
	tests := []struct {
		name          string
		files         map[string]string // filename -> content
		workerCount   int
		expectedCodes []string
		expectedError bool
		errorContains string
		checkProgress bool
	}{
		{
			name: "NormalCase",
			files: map[string]string{
				"codes1.txt": "HAPPYHRS\nFIFTYOFF\nSHORT\nVERYLONGCODE123\nTESTCODE1",
				"codes2.txt": "HAPPYHRS\nSUPER100\nSHORT\nTESTCODE2\nVERYLONGCODE123",
				"codes3.txt": "FIFTYOFF\nSUPER100\nTESTCODE3\nALSOLONG",
			},
			workerCount:   0, // Default
			expectedCodes: []string{"FIFTYOFF", "HAPPYHRS", "SUPER100"},
		},
		{
			name:          "EmptyDirectory",
			files:         map[string]string{},
			workerCount:   0,
			expectedError: true,
		},
		{
			name: "LengthFiltering",
			files: map[string]string{
				"codes1.txt": "AB\nABCDEFG\nGOODCODE\nVERYLONGCODE123\nPERFECT10",
			},
			workerCount:   1,
			expectedCodes: nil, // Need 2+ files for validity, we only have 1, plus length filtering
		},
		{
			name: "LargeWorkerCount",
			files: map[string]string{
				"codes1.txt": "TESTCODE\nGOODCODE",
				"codes2.txt": "TESTCODE\nGOODCODE",
			},
			workerCount:   100,
			expectedCodes: []string{"GOODCODE", "TESTCODE"},
		},
		{
			name: "WithProgress",
			files: map[string]string{
				"codes1.txt": "TESTCODE",
				"codes2.txt": "TESTCODE",
			},
			workerCount:   0,
			expectedCodes: []string{"TESTCODE"},
			checkProgress: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			if len(tt.files) > 0 {
				for filename, content := range tt.files {
					path := filepath.Join(tmpDir, filename)
					err := os.WriteFile(path, []byte(content), 0644)
					require.NoError(t, err, "Failed to create test file %s", filename)
				}
			} else if tt.name == "EmptyDirectory" {
				// Do nothing, directory is empty
			}

			var progressCalled bool
			progressCallback := func(msg string) {
				progressCalled = true
			}

			var callback func(string)
			if tt.checkProgress {
				callback = progressCallback
			}

			validCodes, err := FindValidCodesHashPartition(tmpDir, callback, tt.workerCount)

			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			sort.Strings(validCodes)
			sort.Strings(tt.expectedCodes)
			assert.Equal(t, tt.expectedCodes, validCodes)

			if tt.checkProgress {
				assert.True(t, progressCalled, "Progress callback should have been called")
			}
		})
	}
}

// TestHashPartition_NonExistentDirectory tests error handling for invalid directory
func TestHashPartition_NonExistentDirectory(t *testing.T) {
	t.Parallel()

	_, err := FindValidCodesHashPartition("/path/that/does/not/exist", nil, 0)
	assert.Error(t, err, "Expected error for non-existent directory")
}

// TestHashPartition_MultipleRuns verifies consistent results across runs
func TestHashPartition_MultipleRuns(t *testing.T) {
	// Create a temporary test directory with multiple files
	tmpDir := t.TempDir()

	// Create test files
	testData := map[string]string{
		"file1.txt": "ABCDEFGH\nTESTCODE1\nSHORT\n",
		"file2.txt": "ABCDEFGH\nTESTCODE2\nVERYLONGCODE123\n",
		"file3.txt": "IJKLMNOP\nTESTCODE1\nABCDEFGH\n",
	}

	for filename, content := range testData {
		path := filepath.Join(tmpDir, filename)
		err := os.WriteFile(path, []byte(content), 0644)
		require.NoError(t, err, "Failed to create test file %s", filename)
	}

	// Run hash partition twice
	codes1, err := FindValidCodesHashPartition(tmpDir, nil, 0)
	require.NoError(t, err, "FindValidCodesHashPartition run 1 should not return error")

	codes2, err := FindValidCodesHashPartition(tmpDir, nil, 0)
	require.NoError(t, err, "FindValidCodesHashPartition run 2 should not return error")

	// Sort both for comparison
	sort.Strings(codes1)
	sort.Strings(codes2)

	// Compare results
	assert.Equal(t, codes1, codes2, "Results should be consistent across runs")
}

// TestHashPartition_DifferentWorkerCounts tests with various worker configurations
func TestHashPartition_DifferentWorkerCounts(t *testing.T) {
	// Create test data
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "codes1.txt")
	content1 := `TESTCODE
GOODCODE
BESTCODE`
	err := os.WriteFile(file1, []byte(content1), 0644)
	require.NoError(t, err, "Failed to create test file 1")

	file2 := filepath.Join(tmpDir, "codes2.txt")
	content2 := `TESTCODE
GOODCODE
BESTCODE`
	err = os.WriteFile(file2, []byte(content2), 0644)
	require.NoError(t, err, "Failed to create test file 2")

	workerCounts := []int{1, 2, 4, 8, -1, 0}
	var baseline []string

	for i, workers := range workerCounts {
		validCodes, err := FindValidCodesHashPartition(tmpDir, nil, workers)
		require.NoError(t, err, "FindValidCodesHashPartition(workers=%d) should not return error", workers)

		sort.Strings(validCodes)

		if i == 0 {
			baseline = validCodes
		} else {
			// Verify all worker counts produce same results
			assert.Len(t, validCodes, len(baseline), "Worker count %d should produce same number of codes", workers)

			assert.Equal(t, baseline, validCodes, "Worker count %d should produce same results", workers)
		}
	}
}

// TestHashCode_Collision tests that different codes can hash to same bucket
func TestHashCode_Collision(t *testing.T) {
	t.Parallel()

	// With small bucket count, we should see collisions
	numBuckets := 10
	codes := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		// Generate diverse codes to ensure distribution across buckets
		codes[i] = "CODE" + string(rune('A'+(i/100)%26)) + string(rune('A'+(i/10)%26)) + string(rune('A'+i%26))
	}

	buckets := make(map[int]int)
	for _, code := range codes {
		bucket := hashCode(code, numBuckets)
		buckets[bucket]++
	}

	// With 1000 codes and 10 buckets, we should have codes in multiple buckets
	assert.GreaterOrEqual(t, len(buckets), 5, "Codes should distribute across at least 5 buckets")

	// Verify we have collisions (multiple codes per bucket)
	for bucket, count := range buckets {
		assert.GreaterOrEqual(t, count, 10, "Bucket %d should have at least 10 codes (showing collisions)", bucket)
	}
}

// Benchmarks

func BenchmarkHashCode(b *testing.B) {
	code := "TESTCODE"
	numBuckets := 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashCode(code, numBuckets)
	}
}

func BenchmarkHashCode_LongString(b *testing.B) {
	code := string(make([]byte, 1000))
	numBuckets := 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashCode(code, numBuckets)
	}
}

func BenchmarkProcessBucket_Small(b *testing.B) {
	tmpDir := b.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// Create a small bucket with 100 codes
	content := ""
	for i := 0; i < 100; i++ {
		content += "CODE" + string(rune('A'+i%26)) + "|0\n"
		content += "CODE" + string(rune('A'+i%26)) + "|1\n"
	}

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		b.Fatalf("Failed to create benchmark bucket file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := processBucket(bucketPath)
		if err != nil {
			b.Fatalf("processBucket() error = %v", err)
		}
	}
}

func BenchmarkProcessBucket_Large(b *testing.B) {
	tmpDir := b.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// Create a large bucket with 10,000 codes
	content := ""
	for i := 0; i < 10000; i++ {
		content += "LARGECODE" + string(rune('A'+i%26)) + "|0\n"
		content += "LARGECODE" + string(rune('A'+i%26)) + "|1\n"
	}

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		b.Fatalf("Failed to create benchmark bucket file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := processBucket(bucketPath)
		if err != nil {
			b.Fatalf("processBucket() error = %v", err)
		}
	}
}

func BenchmarkFindValidCodesHashPartition(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 5 test files with 1,000 codes each
	for fileIdx := 0; fileIdx < 5; fileIdx++ {
		filename := filepath.Join(tmpDir, "codes"+string(rune('0'+fileIdx))+".txt")
		content := ""
		for i := 0; i < 1000; i++ {
			content += "BENCHCODE" + string(rune('A'+i%26)) + "\n"
		}
		err := os.WriteFile(filename, []byte(content), 0644)
		if err != nil {
			b.Fatalf("Failed to create benchmark file: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := FindValidCodesHashPartition(tmpDir, nil, 0)
		if err != nil {
			b.Fatalf("FindValidCodesHashPartition() error = %v", err)
		}
	}
}
