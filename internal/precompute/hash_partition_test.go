package precompute

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestHashCode verifies the hash function distributes codes evenly
func TestHashCode(t *testing.T) {
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
		if bucket < 0 || bucket >= numBuckets {
			t.Errorf("hashCode(%s, %d) = %d, want 0 <= bucket < %d", code, numBuckets, bucket, numBuckets)
		}
		bucketCounts[bucket]++
	}

	// Verify we get some distribution (not all in one bucket)
	if len(bucketCounts) < 2 {
		t.Errorf("Expected codes to distribute across multiple buckets, got only %d buckets", len(bucketCounts))
	}

	// Verify deterministic: same code always hashes to same bucket
	for _, code := range testCodes {
		bucket1 := hashCode(code, numBuckets)
		bucket2 := hashCode(code, numBuckets)
		if bucket1 != bucket2 {
			t.Errorf("hashCode(%s) not deterministic: got %d and %d", code, bucket1, bucket2)
		}
	}
}

// TestProcessBucket tests processing a single bucket file
func TestProcessBucket(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// Create test bucket file
	content := `HAPPYHRS|0
HAPPYHRS|1
FIFTYOFF|0
FIFTYOFF|2
TESTCODE|0
SHORTCD|1
SHORTCD|2
`
	err := os.WriteFile(bucketPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	sort.Strings(validCodes)

	// Expected valid codes:
	// HAPPYHRS - in files 0,1 ✓
	// FIFTYOFF - in files 0,2 ✓
	// SHORTCD - in files 1,2 ✓
	// TESTCODE - only in file 0 ✗

	expected := []string{"FIFTYOFF", "HAPPYHRS", "SHORTCD"}
	sort.Strings(expected)

	if len(validCodes) != len(expected) {
		t.Errorf("Expected %d valid codes, got %d", len(expected), len(validCodes))
		t.Errorf("Expected: %v", expected)
		t.Errorf("Got: %v", validCodes)
		return
	}

	for i, code := range expected {
		if validCodes[i] != code {
			t.Errorf("Expected code %s at position %d, got %s", code, i, validCodes[i])
		}
	}
}

// TestProcessBucket_EmptyFile tests processing an empty bucket
func TestProcessBucket_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

	// Create empty bucket file
	err := os.WriteFile(bucketPath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to create test bucket file: %v", err)
	}

	validCodes, err := processBucket(bucketPath)
	if err != nil {
		t.Fatalf("processBucket() error = %v", err)
	}

	if len(validCodes) != 0 {
		t.Errorf("Expected 0 valid codes from empty bucket, got %d", len(validCodes))
	}
}

// TestFindValidCodesHashPartition tests the full hash partition algorithm
func TestFindValidCodesHashPartition(t *testing.T) {
	// Create a temporary test directory with multiple files
	tmpDir := t.TempDir()

	// File 1
	file1 := filepath.Join(tmpDir, "codes1.txt")
	content1 := `HAPPYHRS
FIFTYOFF
SHORT
VERYLONGCODE123
TESTCODE1`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 1: %v", err)
	}

	// File 2
	file2 := filepath.Join(tmpDir, "codes2.txt")
	content2 := `HAPPYHRS
SUPER100
SHORT
TESTCODE2
VERYLONGCODE123`
	err = os.WriteFile(file2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 2: %v", err)
	}

	// File 3
	file3 := filepath.Join(tmpDir, "codes3.txt")
	content3 := `FIFTYOFF
SUPER100
TESTCODE3
ALSOLONG`
	err = os.WriteFile(file3, []byte(content3), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 3: %v", err)
	}

	validCodes, err := FindValidCodesHashPartition(tmpDir, nil)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() error = %v", err)
	}

	// Sort for consistent comparison
	sort.Strings(validCodes)

	// Expected valid codes:
	// HAPPYHRS - 8 chars, in files 1,2 ✓
	// FIFTYOFF - 8 chars, in files 1,3 ✓
	// SUPER100 - 8 chars, in files 2,3 ✓
	// SHORT - 5 chars, in files 1,2 ✗ (too short)
	// VERYLONGCODE123 - 15 chars, in files 1,2 ✗ (too long)
	// TESTCODE1/2/3 - 9 chars, but only in 1 file each ✗

	expected := []string{"FIFTYOFF", "HAPPYHRS", "SUPER100"}
	sort.Strings(expected)

	if len(validCodes) != len(expected) {
		t.Errorf("Expected %d valid codes, got %d", len(expected), len(validCodes))
		t.Errorf("Expected: %v", expected)
		t.Errorf("Got: %v", validCodes)
		return
	}

	for i, code := range expected {
		if validCodes[i] != code {
			t.Errorf("Expected code %s at position %d, got %s", code, i, validCodes[i])
		}
	}
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
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", filename, err)
		}
	}

	// Run hash partition twice
	codes1, err := FindValidCodesHashPartition(tmpDir, nil)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() run 1 error = %v", err)
	}

	codes2, err := FindValidCodesHashPartition(tmpDir, nil)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() run 2 error = %v", err)
	}

	// Sort both for comparison
	sort.Strings(codes1)
	sort.Strings(codes2)

	// Compare results
	if len(codes1) != len(codes2) {
		t.Errorf("Result mismatch: run 1 found %d codes, run 2 found %d codes",
			len(codes1), len(codes2))
		t.Errorf("Run 1: %v", codes1)
		t.Errorf("Run 2: %v", codes2)
		return
	}

	for i := range codes1 {
		if codes1[i] != codes2[i] {
			t.Errorf("Code mismatch at position %d: run1=%s, run2=%s",
				i, codes1[i], codes2[i])
		}
	}
}

// TestHashPartition_EmptyDirectory tests error handling for empty directory
func TestHashPartition_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := FindValidCodesHashPartition(tmpDir, nil)
	if err == nil {
		t.Error("Expected error for empty directory, got nil")
	}
}

// TestHashPartition_WithProgress tests that progress callback is called
func TestHashPartition_WithProgress(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a simple test file
	file1 := filepath.Join(tmpDir, "codes1.txt")
	content1 := `TESTCODE`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	file2 := filepath.Join(tmpDir, "codes2.txt")
	content2 := `TESTCODE`
	err = os.WriteFile(file2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var messages []string
	progressCallback := func(msg string) {
		messages = append(messages, msg)
	}

	_, err = FindValidCodesHashPartition(tmpDir, progressCallback)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() error = %v", err)
	}

	if len(messages) == 0 {
		t.Error("Expected progress messages, got none")
	}
}
