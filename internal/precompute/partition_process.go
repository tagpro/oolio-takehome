package precompute

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// codeInfo tracks file indices and validation status for a code
type codeInfo struct {
	fileIndices map[int]struct{}
	isValid     bool
}

// processBucket processes a single bucket file to find valid codes
// Optimized single-pass approach: builds valid codes list as we read
func processBucket(bucketPath string) ([]string, error) {
	f, err := os.Open(bucketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket file %s: %w", bucketPath, err)
	}
	defer f.Close()

	codeMap := make(map[string]*codeInfo)
	var validCodes []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// Parse line: "code|fileIndex"
		parts := strings.Split(line, "|")
		if len(parts) != 2 {
			continue // Skip malformed lines
		}

		code := parts[0]
		fileIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			continue // Skip lines with invalid file index
		}

		// Get or create code info
		info := codeMap[code]
		if info == nil {
			info = &codeInfo{fileIndices: make(map[int]struct{})}
			codeMap[code] = info
		}

		// Only track file indices if not yet confirmed valid
		if !info.isValid {
			info.fileIndices[fileIdx] = struct{}{}

			// As soon as we see 2+ files, mark as valid!
			if len(info.fileIndices) >= 2 {
				info.isValid = true
				validCodes = append(validCodes, code)
				info.fileIndices = nil // Free memory immediately!
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading bucket file %s: %w", bucketPath, err)
	}

	return validCodes, nil
}

func processBucketsWorker(id int, bucketPath <-chan string, results chan<- []string) error {
	fmt.Println("Worker", id, "started")
	processCount := 0
	for path := range bucketPath {
		processCount++
		validCodes, err := processBucket(path)
		if err != nil {
			return err
		}
		results <- validCodes
	}
	fmt.Println("Worker", id, "finished after processing", processCount, "buckets")
	return nil
}
