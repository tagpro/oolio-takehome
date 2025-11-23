package precompute

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// TestProcessBucket_SingleFileOnly tests codes appearing in only one file
func TestProcessBucket_SingleFileOnly(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// All codes appear in only one file
	content := `CODE1|0
CODE2|1
CODE3|2
CODE4|0
CODE5|1`

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	if len(validCodes) != 0 {
		t.Errorf("Expected 0 valid codes, got %d: %v", len(validCodes), validCodes)
	}
}

// TestProcessBucket_DuplicateEntries tests handling of duplicate code-file pairs
func TestProcessBucket_DuplicateEntries(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// CODE1 appears multiple times in same files
	content := `CODE1|0
CODE1|1
CODE1|0
CODE1|1
CODE1|2`

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	// CODE1 should be valid (appears in files 0, 1, 2)
	if len(validCodes) != 1 {
		t.Errorf("Expected 1 valid code, got %d", len(validCodes))
	}

	if len(validCodes) > 0 && validCodes[0] != "CODE1" {
		t.Errorf("Expected CODE1, got %s", validCodes[0])
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
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	// All 1,000 codes should be valid
	if len(validCodes) != numCodes {
		t.Errorf("Expected %d valid codes, got %d", numCodes, len(validCodes))
	}
}

// TestProcessBucket_MixedValidInvalid tests mix of valid and invalid codes
func TestProcessBucket_MixedValidInvalid(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	content := `VALID1|0
VALID1|1
INVALID1|0
VALID2|0
VALID2|1
VALID2|2
INVALID2|1
VALID3|0
VALID3|1`

	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	sort.Strings(validCodes)

	expected := []string{"VALID1", "VALID2", "VALID3"}
	sort.Strings(expected)

	if len(validCodes) != len(expected) {
		t.Errorf("Expected %d valid codes, got %d", len(expected), len(validCodes))
		return
	}

	for i, code := range expected {
		if validCodes[i] != code {
			t.Errorf("Expected code %s at position %d, got %s", code, i, validCodes[i])
		}
	}
}

// TestProcessBucketsWorker tests the worker function
func TestProcessBucketsWorker(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple bucket files
	bucket1 := filepath.Join(tmpDir, "bucket_001.txt")
	content1 := `CODE1|0
CODE1|1
CODE2|0
CODE2|1`
	err := os.WriteFile(bucket1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket 1: %v", err)
	}

	bucket2 := filepath.Join(tmpDir, "bucket_002.txt")
	content2 := `CODE3|0
CODE3|1
CODE4|0`
	err = os.WriteFile(bucket2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket 2: %v", err)
	}

	bucket3 := filepath.Join(tmpDir, "bucket_003.txt")
	content3 := `CODE5|0
CODE5|1
CODE5|2`
	err = os.WriteFile(bucket3, []byte(content3), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket 3: %v", err)
	}

	// Set up channels
	bucketPaths := make(chan string, 3)
	results := make(chan []string, 3)

	// Send bucket paths
	bucketPaths <- bucket1
	bucketPaths <- bucket2
	bucketPaths <- bucket3
	close(bucketPaths)

	// Run worker
	err = processBucketsWorker(1, bucketPaths, results)
	if err != nil {
		t.Fatalf("processBucketsWorker() error = %v", err)
	}
	close(results)

	// Collect all results
	var allCodes []string
	for codes := range results {
		allCodes = append(allCodes, codes...)
	}

	sort.Strings(allCodes)

	// Expected: CODE1, CODE2, CODE3, CODE5 (CODE4 only in 1 file)
	expected := []string{"CODE1", "CODE2", "CODE3", "CODE5"}
	sort.Strings(expected)

	if len(allCodes) != len(expected) {
		t.Errorf("Expected %d codes, got %d", len(expected), len(allCodes))
		t.Errorf("Expected: %v", expected)
		t.Errorf("Got: %v", allCodes)
		return
	}

	for i, code := range expected {
		if allCodes[i] != code {
			t.Errorf("Expected code %s at position %d, got %s", code, i, allCodes[i])
		}
	}
}

// TestProcessBucketsWorker_EmptyChannel tests worker with no buckets
func TestProcessBucketsWorker_EmptyChannel(t *testing.T) {
	bucketPaths := make(chan string)
	results := make(chan []string, 1)

	// Close immediately - no work to do
	close(bucketPaths)

	err := processBucketsWorker(1, bucketPaths, results)
	if err != nil {
		t.Fatalf("processBucketsWorker() error = %v", err)
	}
	close(results)

	// Should receive no results
	count := 0
	for range results {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 results from empty channel, got %d", count)
	}
}

// TestProcessBucketsWorker_ErrorHandling tests worker error handling
func TestProcessBucketsWorker_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()

	// Create one valid bucket
	bucket1 := filepath.Join(tmpDir, "bucket_001.txt")
	content1 := `CODE1|0
CODE1|1`
	err := os.WriteFile(bucket1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket 1: %v", err)
	}

	// Reference a non-existent bucket (will cause error)
	bucket2 := filepath.Join(tmpDir, "nonexistent_bucket.txt")

	bucketPaths := make(chan string, 2)
	results := make(chan []string, 2)

	bucketPaths <- bucket1
	bucketPaths <- bucket2
	close(bucketPaths)

	err = processBucketsWorker(1, bucketPaths, results)
	if err == nil {
		t.Error("Expected error when processing non-existent bucket, got nil")
	}
}

// TestProcessBucketsWorker_MultipleWorkers tests concurrent workers
func TestProcessBucketsWorker_MultipleWorkers(t *testing.T) {
	tmpDir := t.TempDir()

	// Create 10 buckets
	numBuckets := 10
	bucketPaths := make(chan string, numBuckets)
	results := make(chan []string, numBuckets)

	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+i%10))+".txt")
		content := "CODE" + string(rune('A'+i)) + "|0\n" +
			"CODE" + string(rune('A'+i)) + "|1\n"
		err := os.WriteFile(bucketPath, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to create bucket %d: %v", i, err)
		}
		bucketPaths <- bucketPath
	}
	close(bucketPaths)

	// Run 3 workers concurrently
	numWorkers := 3
	errors := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			errors <- processBucketsWorker(workerID, bucketPaths, results)
		}()
	}

	// Wait for all workers
	for w := 0; w < numWorkers; w++ {
		if err := <-errors; err != nil {
			t.Fatalf("Worker error: %v", err)
		}
	}
	close(results)

	// Collect all results
	var allCodes []string
	for codes := range results {
		allCodes = append(allCodes, codes...)
	}

	// Should have 10 valid codes (one from each bucket)
	if len(allCodes) != numBuckets {
		t.Errorf("Expected %d codes from %d buckets, got %d",
			numBuckets, numBuckets, len(allCodes))
	}
}

// TestProcessBucketsWorker_StressTest tests workers under high load
func TestProcessBucketsWorker_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()

	// Create 1000 bucket files
	numBuckets := 1000
	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+(i%10)))+string(rune('0'+(i/10)%10))+string(rune('0'+(i/100)%10))+".txt")
		content := ""
		// Add 10 codes per bucket
		for j := 0; j < 10; j++ {
			code := "STRESS" + string(rune('A'+(i+j)%26))
			content += code + "|0\n"
			content += code + "|1\n"
		}
		err := os.WriteFile(bucketPath, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to create bucket %d: %v", i, err)
		}
	}

	bucketPaths := make(chan string, numBuckets)
	results := make(chan []string, numBuckets)

	// Fill bucket paths
	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tmpDir, "bucket_"+string(rune('0'+(i%10)))+string(rune('0'+(i/10)%10))+string(rune('0'+(i/100)%10))+".txt")
		bucketPaths <- bucketPath
	}
	close(bucketPaths)

	// Run 10 workers concurrently
	numWorkers := 10
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
			t.Fatalf("Worker error: %v", err)
		}
	}
	close(results)

	// Collect and verify results
	totalCodes := 0
	for codes := range results {
		totalCodes += len(codes)
	}

	// We expect 10,000 valid codes (1000 buckets * 10 codes each, all valid)
	if totalCodes != 10000 {
		t.Errorf("Expected 10000 total codes, got %d", totalCodes)
	}
}

// TestProcessBucket_OrderIndependence tests that bucket processing order doesn't matter
func TestProcessBucket_OrderIndependence(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Create bucket with codes in different orders
	bucket1Path := filepath.Join(tmpDir, "bucket1.txt")
	content1 := `CODE1|0
CODE1|1
CODE2|0
CODE2|1
CODE3|0
CODE3|1`

	bucket2Path := filepath.Join(tmpDir, "bucket2.txt")
	content2 := `CODE3|1
CODE3|0
CODE1|1
CODE1|0
CODE2|1
CODE2|0`

	err := os.WriteFile(bucket1Path, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket1: %v", err)
	}

	err = os.WriteFile(bucket2Path, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create bucket2: %v", err)
	}

	// Process both buckets
	codes1, err := processBucket(bucket1Path)
	if err != nil {
		t.Fatalf("processBucket(bucket1) error = %v", err)
	}

	codes2, err := processBucket(bucket2Path)
	if err != nil {
		t.Fatalf("processBucket(bucket2) error = %v", err)
	}

	// Sort for comparison
	sort.Strings(codes1)
	sort.Strings(codes2)

	// Should produce identical results regardless of order
	if !reflect.DeepEqual(codes1, codes2) {
		t.Errorf("Bucket processing not order-independent:\n  bucket1: %v\n  bucket2: %v",
			codes1, codes2)
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
