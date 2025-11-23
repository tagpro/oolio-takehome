package precompute

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"golang.org/x/sync/errgroup"
)

const (
	// Number of hash buckets for partitioning codes
	numBuckets = 1000

	// Scanner buffer sizes for reading files
	scannerInitialBuffer = 64 * 1024   // 64 KB
	scannerMaxBuffer     = 1024 * 1024 // 1 MB

	// Progress reporting interval for partitioning phase
	progressReportInterval = 10_000_000 // Report every 10M codes
)

// hashCode hashes a string code to a bucket number using FNV-1a
func hashCode(code string, numBuckets int) int {
	h := fnv.New32a()
	h.Write([]byte(code))
	return int(h.Sum32() % uint32(numBuckets))
}

// FindValidCodesHashPartition uses hash-based partitioning to find valid promo codes.
// This approach partitions codes into buckets, processes each bucket independently.
//
// A code is valid if:
// 1. It appears in at least 2 files
// 2. Its length is between 8 and 10 characters (inclusive)
//
// workers: Number of parallel workers for bucket processing. If 0 or negative, uses runtime.NumCPU().
func FindValidCodesHashPartition(dirPath string, progressCallback func(string), workers int) ([]string, error) {

	// Get list of files in directory
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, filepath.Join(dirPath, entry.Name()))
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files found in directory %s", dirPath)
	}

	// Create temporary directory for bucket files
	tempDir, err := os.MkdirTemp("", "hash_partition_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Phase 1: Partition files into buckets
	if progressCallback != nil {
		progressCallback("Phase 1: Partitioning files into buckets...")
	}

	if err := partitionFiles(files, numBuckets, tempDir, progressCallback); err != nil {
		return nil, err
	}

	// Phase 2: Process each bucket to find valid codes
	if progressCallback != nil {
		progressCallback("Phase 2: Processing buckets to find valid codes...")
	}

	validCodes, err := processBuckets(numBuckets, tempDir, progressCallback, workers)
	if err != nil {
		return nil, err
	}

	if progressCallback != nil {
		progressCallback(fmt.Sprintf("Found %d valid codes", len(validCodes)))
	}

	return validCodes, nil
}

// partitionFiles partitions all input files into bucket files
func partitionFiles(files []string, numBuckets int, tempDir string, progressCallback func(string)) error {
	// Create bucket file handles
	bucketFiles := make([]*os.File, numBuckets)
	bucketWriters := make([]*bufio.Writer, numBuckets)

	for i := 0; i < numBuckets; i++ {
		bucketPath := filepath.Join(tempDir, fmt.Sprintf("bucket_%03d.txt", i))
		f, err := os.Create(bucketPath)
		if err != nil {
			// Close any already opened files
			for j := 0; j < i; j++ {
				bucketWriters[j].Flush()
				bucketFiles[j].Close()
			}
			return fmt.Errorf("failed to create bucket file %d: %w", i, err)
		}
		bucketFiles[i] = f
		bucketWriters[i] = bufio.NewWriter(f)
	}

	// Ensure all bucket files are closed at the end
	defer func() {
		for i := 0; i < numBuckets; i++ {
			bucketWriters[i].Flush()
			bucketFiles[i].Close()
		}
	}()

	// Process each input file
	totalCodesRead := 0
	totalCodesPartitioned := 0

	for fileIdx, filename := range files {
		if progressCallback != nil {
			progressCallback(fmt.Sprintf("  Partitioning file %d/%d: %s", fileIdx+1, len(files), filepath.Base(filename)))
		}

		f, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filename, err)
		}

		scanner := bufio.NewScanner(f)
		buf := make([]byte, 0, scannerInitialBuffer)
		scanner.Buffer(buf, scannerMaxBuffer)

		fileCodesRead := 0
		fileCodesPartitioned := 0

		for scanner.Scan() {
			code := scanner.Text()
			fileCodesRead++
			totalCodesRead++

			// Skip empty lines
			if code == "" {
				continue
			}

			// Filter: only partition codes with length 8-10
			if len(code) < 8 || len(code) > 10 {
				continue
			}

			// Hash to bucket
			bucketNum := hashCode(code, numBuckets)

			// Write to bucket file: "code|fileIndex\n"
			if _, err := bucketWriters[bucketNum].WriteString(fmt.Sprintf("%s|%d\n", code, fileIdx)); err != nil {
				f.Close()
				return fmt.Errorf("failed to write to bucket %d: %w", bucketNum, err)
			}

			fileCodesPartitioned++
			totalCodesPartitioned++

			// Report progress periodically
			if progressCallback != nil && fileCodesRead%progressReportInterval == 0 {
				progressCallback(fmt.Sprintf("    Processed %dM codes (%dM valid length)",
					fileCodesRead/1_000_000, fileCodesPartitioned/1_000_000))
			}
		}

		f.Close()

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading file %s: %w", filename, err)
		}

		if progressCallback != nil {
			progressCallback(fmt.Sprintf("    File %d complete: %d codes read, %d codes partitioned (8-10 chars)",
				fileIdx+1, fileCodesRead, fileCodesPartitioned))
		}
	}

	// Flush all bucket writers
	for i := 0; i < numBuckets; i++ {
		if err := bucketWriters[i].Flush(); err != nil {
			return fmt.Errorf("failed to flush bucket %d: %w", i, err)
		}
	}

	if progressCallback != nil {
		progressCallback(fmt.Sprintf("  Partitioning complete: %d total codes read, %d codes partitioned into %d buckets",
			totalCodesRead, totalCodesPartitioned, numBuckets))
	}

	return nil
}

// processBuckets processes all bucket files to find valid codes
// Uses a worker pool for parallel processing
func processBuckets(numBuckets int, tempDir string, progressCallback func(string), workers int) ([]string, error) {
	// Use runtime.NumCPU() if workers is 0 or negative
	workerPoolSize := workers
	if workerPoolSize <= 0 {
		workerPoolSize = runtime.NumCPU()
	}

	bucketPaths := make(chan string, numBuckets)
	results := make(chan []string, workerPoolSize)

	// Start worker pool
	var eg errgroup.Group
	for w := 1; w <= workerPoolSize; w++ {
		eg.Go(func() error {
			return processBucketsWorker(w, bucketPaths, results)
		})
	}

	// Send all bucket paths to workers
	bucketsProcessed := 0
	for bucketNum := 0; bucketNum < numBuckets; bucketNum++ {
		bucketPath := filepath.Join(tempDir, fmt.Sprintf("bucket_%03d.txt", bucketNum))

		// Check if bucket file exists and is not empty
		info, err := os.Stat(bucketPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue // Skip empty buckets
			}
			return nil, fmt.Errorf("failed to stat bucket file %d: %w", bucketNum, err)
		}

		if info.Size() == 0 {
			continue // Skip empty buckets
		}

		bucketPaths <- bucketPath
		bucketsProcessed++
	}
	close(bucketPaths)

	// Collect results in a separate goroutine
	var allValidCodes []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		resultCount := 0
		for codes := range results {
			allValidCodes = append(allValidCodes, codes...)
			resultCount++

			// Report progress every 100 results or when complete
			if progressCallback != nil && (resultCount%100 == 0 || resultCount == bucketsProcessed) {
				progressCallback(fmt.Sprintf("    Processed %d/%d buckets (%d valid codes found so far)",
					resultCount, bucketsProcessed, len(allValidCodes)))
			}
		}
	}()

	// Wait for all workers to finish
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Close results channel to signal collector we're done
	close(results)

	// Wait for result collector to finish
	<-done

	// Sort codes alphabetically for consistent output
	sort.Strings(allValidCodes)

	if progressCallback != nil {
		progressCallback(fmt.Sprintf("  Processing complete: %d buckets processed, %d valid codes found",
			bucketsProcessed, len(allValidCodes)))
	}

	return allValidCodes, nil
}
