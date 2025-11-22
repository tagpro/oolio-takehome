package precompute

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

// LoadFile reads all lines from a file and returns them as a slice.
// Each line is trimmed of whitespace.
func LoadFile(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer f.Close()

	var codes []string
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		code := scanner.Text()
		if code != "" {
			codes = append(codes, code)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filename, err)
	}

	return codes, nil
}

// LoadDirectory reads all files from a directory and returns a map
// where keys are codes and values are slices of file indices (0-based)
// indicating which files contain that code.
func LoadDirectory(dirPath string) (map[string][]int, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	codeToFiles := make(map[string][]int)
	fileIndex := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := filepath.Join(dirPath, entry.Name())
		codes, err := LoadFile(filename)
		if err != nil {
			return nil, err
		}

		for _, code := range codes {
			codeToFiles[code] = append(codeToFiles[code], fileIndex)
		}

		fileIndex++
	}

	return codeToFiles, nil
}

// DeduplicateFileIndices removes duplicate file indices from the slice
// while preserving order.
func DeduplicateFileIndices(indices []int) []int {
	seen := make(map[int]bool)
	result := make([]int, 0, len(indices))

	for _, idx := range indices {
		if !seen[idx] {
			seen[idx] = true
			result = append(result, idx)
		}
	}

	return result
}
