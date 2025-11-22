package precompute

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteTextFile(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "test_codes.txt")

	testCodes := []string{"HAPPYHRS", "FIFTYOFF", "SUPER100"}

	err := WriteTextFile(testCodes, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	// Verify the file was created
	if _, err := os.Stat(txtPath); os.IsNotExist(err) {
		t.Fatal("Text file was not created")
	}

	// Read the file and verify contents
	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read text file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != len(testCodes) {
		t.Errorf("Expected %d lines, got %d", len(testCodes), len(lines))
	}

	for i, expectedCode := range testCodes {
		if lines[i] != expectedCode {
			t.Errorf("Expected code %s at line %d, got %s", expectedCode, i+1, lines[i])
		}
	}
}

func TestWriteTextFile_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "empty_codes.txt")

	err := WriteTextFile([]string{}, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	// Read the file
	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read text file: %v", err)
	}

	if len(content) != 0 {
		t.Errorf("Expected empty file, got %d bytes", len(content))
	}
}
