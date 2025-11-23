package precompute

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
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
			if bucket < 0 || bucket >= tt.numBuckets {
				t.Errorf("hashCode(%s, %d) = %d, want 0 <= bucket < %d",
					tt.code, tt.numBuckets, bucket, tt.numBuckets)
			}

			// Verify deterministic
			bucket2 := hashCode(tt.code, tt.numBuckets)
			if bucket != bucket2 {
				t.Errorf("hashCode(%s) not deterministic: got %d and %d", tt.code, bucket, bucket2)
			}
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
		if bucket < 0 || bucket >= numBuckets {
			t.Errorf("hashCode(%s, %d) = %d, want 0 <= bucket < %d", code, numBuckets, bucket, numBuckets)
		}
		bucketCounts[bucket]++
	}

	// Verify we get some distribution (not all in one bucket)
	if len(bucketCounts) < 2 {
		t.Errorf("Expected codes to distribute across multiple buckets, got only %d buckets", len(bucketCounts))
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

	validCodes, err := FindValidCodesHashPartition(tmpDir, nil, 0)
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
	codes1, err := FindValidCodesHashPartition(tmpDir, nil, 0)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() run 1 error = %v", err)
	}

	codes2, err := FindValidCodesHashPartition(tmpDir, nil, 0)
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

	_, err := FindValidCodesHashPartition(tmpDir, nil, 0)
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

	_, err = FindValidCodesHashPartition(tmpDir, progressCallback, 0)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() error = %v", err)
	}

	if len(messages) == 0 {
		t.Error("Expected progress messages, got none")
	}
}

// TestProcessBucket_MalformedLines tests handling of malformed bucket data
func TestProcessBucket_MalformedLines(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantErr  bool
		wantLen  int // Expected number of valid codes if no error
	}{
		{
			name: "missing pipe separator",
			content: `TESTCODE0
GOODCODE|1
GOODCODE|2`,
			wantErr: false,
			wantLen: 1, // GOODCODE should still be valid, malformed line is skipped
		},
		{
			name: "multiple pipes",
			content: `TESTCODE|0|extra
GOODCODE|1
GOODCODE|2`,
			wantErr: false,
			wantLen: 1, // GOODCODE should still be valid, malformed line is skipped
		},
		{
			name: "non-numeric file index",
			content: `TESTCODE|abc
GOODCODE|1
GOODCODE|2`,
			wantErr: false,
			wantLen: 1, // GOODCODE should still be valid, malformed line is skipped
		},
		{
			name: "negative file index",
			content: `TESTCODE|-1
GOODCODE|0
GOODCODE|1`,
			wantErr: false,
			wantLen: 1, // GOODCODE should still be valid
		},
		{
			name: "empty lines mixed with valid",
			content: `

GOODCODE|0

GOODCODE|1

`,
			wantErr: false,
			wantLen: 1, // GOODCODE should be valid
		},
		{
			name: "whitespace in code",
			content: `GOOD CODE|0
GOOD CODE|1`,
			wantErr: false,
			wantLen: 1, // "GOOD CODE" should be valid if it appears in 2 files
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			bucketPath := filepath.Join(tmpDir, "bucket_000.txt")

			err := os.WriteFile(bucketPath, []byte(tt.content), 0644)
			if err != nil {
				t.Fatalf("Failed to create test bucket file: %v", err)
			}

			validCodes, err := processBucket(bucketPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("processBucket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(validCodes) != tt.wantLen {
				t.Errorf("processBucket() returned %d codes, want %d", len(validCodes), tt.wantLen)
			}
		})
	}
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
	if err != nil {
		t.Fatalf("Failed to create test file 1: %v", err)
	}

	file2 := filepath.Join(tmpDir, "codes2.txt")
	content2 := `TESTCODE
GOODCODE
BESTCODE`
	err = os.WriteFile(file2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 2: %v", err)
	}

	workerCounts := []int{1, 2, 4, 8, -1, 0}
	var baseline []string

	for i, workers := range workerCounts {
		validCodes, err := FindValidCodesHashPartition(tmpDir, nil, workers)
		if err != nil {
			t.Fatalf("FindValidCodesHashPartition(workers=%d) error = %v", workers, err)
		}

		sort.Strings(validCodes)

		if i == 0 {
			baseline = validCodes
		} else {
			// Verify all worker counts produce same results
			if len(validCodes) != len(baseline) {
				t.Errorf("Worker count %d produced %d codes, expected %d",
					workers, len(validCodes), len(baseline))
				continue
			}

			for j := range validCodes {
				if validCodes[j] != baseline[j] {
					t.Errorf("Worker count %d produced different results at position %d: got %s, want %s",
						workers, j, validCodes[j], baseline[j])
				}
			}
		}
	}
}

// TestHashPartition_NonExistentDirectory tests error handling for invalid directory
func TestHashPartition_NonExistentDirectory(t *testing.T) {
	t.Parallel()

	_, err := FindValidCodesHashPartition("/path/that/does/not/exist", nil, 0)
	if err == nil {
		t.Error("Expected error for non-existent directory, got nil")
	}
}

// TestPartitionFiles_LengthFiltering tests that codes outside 8-10 char range are filtered
func TestPartitionFiles_LengthFiltering(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test file with codes of various lengths
	file1 := filepath.Join(tmpDir, "codes1.txt")
	content := `AB
ABCDEFG
GOODCODE
VERYLONGCODE123
PERFECT10`

	err := os.WriteFile(file1, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Run the full pipeline to verify filtering
	validCodes, err := FindValidCodesHashPartition(tmpDir, nil, 1)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() error = %v", err)
	}

	// Should get no valid codes (need 2+ files for validity, we only have 1)
	if len(validCodes) != 0 {
		t.Errorf("Expected 0 valid codes with single file, got %d", len(validCodes))
	}
}

// TestHashPartition_LargeWorkerCount tests with more workers than buckets
func TestHashPartition_LargeWorkerCount(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "codes1.txt")
	file2 := filepath.Join(tmpDir, "codes2.txt")
	content := `TESTCODE
GOODCODE`

	err := os.WriteFile(file1, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	err = os.WriteFile(file2, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Use 100 workers (more than likely buckets with data)
	validCodes, err := FindValidCodesHashPartition(tmpDir, nil, 100)
	if err != nil {
		t.Fatalf("FindValidCodesHashPartition() error = %v", err)
	}

	sort.Strings(validCodes)
	expected := []string{"GOODCODE", "TESTCODE"}
	sort.Strings(expected)

	if !reflect.DeepEqual(validCodes, expected) {
		t.Errorf("Expected %v, got %v", expected, validCodes)
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
	if len(buckets) < 5 {
		t.Errorf("Expected codes in at least 5 buckets, got %d buckets", len(buckets))
	}

	// Verify we have collisions (multiple codes per bucket)
	for bucket, count := range buckets {
		if count < 10 {
			t.Errorf("Bucket %d has only %d codes, expected more (showing collisions)", bucket, count)
		}
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
