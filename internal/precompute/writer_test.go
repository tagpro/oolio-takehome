package precompute

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteTextFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		codes     []string
		wantLines int
		wantErr   bool
	}{
		{
			name:      "multiple codes",
			codes:     []string{"HAPPYHRS", "FIFTYOFF", "SUPER100"},
			wantLines: 3,
			wantErr:   false,
		},
		{
			name:      "empty slice",
			codes:     []string{},
			wantLines: 0,
			wantErr:   false,
		},
		{
			name:      "single code",
			codes:     []string{"SINGLECODE"},
			wantLines: 1,
			wantErr:   false,
		},
		{
			name:      "codes with special characters",
			codes:     []string{"CODE@123", "TEST-CODE", "CODE_WITH_UNDERSCORE"},
			wantLines: 3,
			wantErr:   false,
		},
		{
			name:      "codes with spaces",
			codes:     []string{"CODE WITH SPACES", "ANOTHER ONE"},
			wantLines: 2,
			wantErr:   false,
		},
		{
			name:      "very long codes",
			codes:     []string{strings.Repeat("A", 1000), strings.Repeat("B", 1000)},
			wantLines: 2,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			txtPath := filepath.Join(tmpDir, "test_codes.txt")

			err := WriteTextFile(tt.codes, txtPath)
			if (err != nil) != tt.wantErr {
				t.Fatalf("WriteTextFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
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

			if tt.wantLines == 0 {
				if len(content) != 0 {
					t.Errorf("Expected empty file, got %d bytes", len(content))
				}
				return
			}

			lines := strings.Split(strings.TrimSpace(string(content)), "\n")
			if len(lines) != tt.wantLines {
				t.Errorf("Expected %d lines, got %d", tt.wantLines, len(lines))
			}

			for i, expectedCode := range tt.codes {
				if i < len(lines) && lines[i] != expectedCode {
					t.Errorf("Expected code %s at line %d, got %s", expectedCode, i+1, lines[i])
				}
			}

			// Verify trailing newline for non-empty files
			if len(tt.codes) > 0 && len(content) > 0 {
				if content[len(content)-1] != '\n' {
					t.Error("Expected trailing newline in non-empty file")
				}
			}
		})
	}
}

func TestWriteTextFile_InvalidPath(t *testing.T) {
	// Try to write to a directory that doesn't exist
	invalidPath := "/nonexistent/directory/that/should/not/exist/codes.txt"

	err := WriteTextFile([]string{"CODE1", "CODE2"}, invalidPath)
	if err == nil {
		t.Error("Expected error when writing to invalid path, got nil")
	}
}

func TestWriteTextFile_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "overwrite.txt")

	// Write initial content
	initialCodes := []string{"CODE1", "CODE2"}
	err := WriteTextFile(initialCodes, txtPath)
	if err != nil {
		t.Fatalf("Initial WriteTextFile() error = %v", err)
	}

	// Overwrite with new content
	newCodes := []string{"NEWCODE1", "NEWCODE2", "NEWCODE3"}
	err = WriteTextFile(newCodes, txtPath)
	if err != nil {
		t.Fatalf("Overwrite WriteTextFile() error = %v", err)
	}

	// Verify new content
	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read text file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != len(newCodes) {
		t.Errorf("Expected %d lines after overwrite, got %d", len(newCodes), len(lines))
	}

	for i, expectedCode := range newCodes {
		if i < len(lines) && lines[i] != expectedCode {
			t.Errorf("Expected code %s at line %d after overwrite, got %s", expectedCode, i+1, lines[i])
		}
	}
}

func TestWriteTextFile_LargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "large.txt")

	// Create 10,000 codes
	codes := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		codes[i] = "CODE" + strings.Repeat("X", 50)
	}

	err := WriteTextFile(codes, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	// Verify file size is reasonable
	info, err := os.Stat(txtPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Each code is ~54 chars + newline, so ~540KB total
	expectedSize := int64(10000 * 55)
	if info.Size() < expectedSize-1000 || info.Size() > expectedSize+1000 {
		t.Errorf("Expected file size around %d bytes, got %d", expectedSize, info.Size())
	}
}

func TestWriteTextFile_PreservesOrder(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "order.txt")

	codes := []string{"ZEBRA", "APPLE", "MANGO", "BANANA"}

	err := WriteTextFile(codes, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read text file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	for i, expectedCode := range codes {
		if i < len(lines) && lines[i] != expectedCode {
			t.Errorf("Order not preserved: expected %s at position %d, got %s", expectedCode, i, lines[i])
		}
	}
}

// Benchmarks

func BenchmarkWriteTextFile_Small(b *testing.B) {
	tmpDir := b.TempDir()
	codes := []string{"CODE1", "CODE2", "CODE3", "CODE4", "CODE5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txtPath := filepath.Join(tmpDir, "bench_small.txt")
		err := WriteTextFile(codes, txtPath)
		if err != nil {
			b.Fatalf("WriteTextFile() error = %v", err)
		}
	}
}

func BenchmarkWriteTextFile_Medium(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 1,000 codes
	codes := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		codes[i] = "CODE" + strings.Repeat("X", 20)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txtPath := filepath.Join(tmpDir, "bench_medium.txt")
		err := WriteTextFile(codes, txtPath)
		if err != nil {
			b.Fatalf("WriteTextFile() error = %v", err)
		}
	}
}

func BenchmarkWriteTextFile_Large(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 10,000 codes
	codes := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		codes[i] = "CODE" + strings.Repeat("X", 50)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txtPath := filepath.Join(tmpDir, "bench_large.txt")
		err := WriteTextFile(codes, txtPath)
		if err != nil {
			b.Fatalf("WriteTextFile() error = %v", err)
		}
	}
}

// TestWriteTextFile_UnicodeContent tests writing codes with unicode characters
func TestWriteTextFile_UnicodeContent(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "unicode.txt")

	codes := []string{"CODE世界", "PROMO™️", "DEAL①②③"}

	err := WriteTextFile(codes, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	// Read back and verify
	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != len(codes) {
		t.Errorf("Expected %d lines, got %d", len(codes), len(lines))
	}

	for i, expectedCode := range codes {
		if i < len(lines) && lines[i] != expectedCode {
			t.Errorf("Unicode code mismatch at line %d: got %q, want %q",
				i+1, lines[i], expectedCode)
		}
	}
}

// TestWriteTextFile_EmptyStrings tests writing empty strings (edge case)
func TestWriteTextFile_EmptyStrings(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "empty_strings.txt")

	// Mix of empty and non-empty strings
	codes := []string{"CODE1", "", "CODE2", "", "CODE3"}

	err := WriteTextFile(codes, txtPath)
	if err != nil {
		t.Fatalf("WriteTextFile() error = %v", err)
	}

	content, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	// Verify all lines are preserved including empty ones
	if len(lines) != len(codes) {
		t.Errorf("Expected %d lines, got %d", len(codes), len(lines))
	}
}
