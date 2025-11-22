package precompute

import (
	"fmt"
	"os"
	"strings"
)

// WriteTextFile writes valid codes to a plain text file.
// Each code is on a separate line.
func WriteTextFile(validCodes []string, outputPath string) error {
	content := strings.Join(validCodes, "\n")
	if len(validCodes) > 0 {
		content += "\n" // Add trailing newline
	}

	if err := os.WriteFile(outputPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write text file: %w", err)
	}

	return nil
}
