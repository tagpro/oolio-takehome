package precompute

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			if tt.wantErr {
				assert.Error(t, err, "WriteTextFile should return error")
				return
			}
			require.NoError(t, err, "WriteTextFile should not return error")

			// Verify the file was created
			_, err = os.Stat(txtPath)
			assert.False(t, os.IsNotExist(err), "Text file should be created")

			// Read the file and verify contents
			content, err := os.ReadFile(txtPath)
			require.NoError(t, err, "Failed to read text file")

			if tt.wantLines == 0 {
				assert.Empty(t, content, "Expected empty file")
				return
			}

			lines := strings.Split(strings.TrimSpace(string(content)), "\n")
			assert.Len(t, lines, tt.wantLines, "Expected %d lines", tt.wantLines)

			for i, expectedCode := range tt.codes {
				if i < len(lines) {
					assert.Equal(t, expectedCode, lines[i], "Expected code at line %d", i+1)
				}
			}

			// Verify trailing newline for non-empty files
			if len(tt.codes) > 0 && len(content) > 0 {
				assert.Equal(t, byte('\n'), content[len(content)-1], "Expected trailing newline in non-empty file")
			}
		})
	}
}

func TestWriteTextFile_InvalidPath(t *testing.T) {
	// Try to write to a directory that doesn't exist
	invalidPath := "/nonexistent/directory/that/should/not/exist/codes.txt"

	err := WriteTextFile([]string{"CODE1", "CODE2"}, invalidPath)
	assert.Error(t, err, "Expected error when writing to invalid path")
}

func TestWriteTextFile_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "overwrite.txt")

	// Write initial content
	initialCodes := []string{"CODE1", "CODE2"}
	err := WriteTextFile(initialCodes, txtPath)
	require.NoError(t, err, "Initial WriteTextFile should not return error")

	// Overwrite with new content
	newCodes := []string{"NEWCODE1", "NEWCODE2", "NEWCODE3"}
	err = WriteTextFile(newCodes, txtPath)
	require.NoError(t, err, "Overwrite WriteTextFile should not return error")

	// Verify new content
	content, err := os.ReadFile(txtPath)
	require.NoError(t, err, "Failed to read text file")

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Len(t, lines, len(newCodes), "Expected %d lines after overwrite", len(newCodes))

	for i, expectedCode := range newCodes {
		if i < len(lines) {
			assert.Equal(t, expectedCode, lines[i], "Expected code at line %d after overwrite", i+1)
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
	require.NoError(t, err, "WriteTextFile should not return error")

	// Verify file size is reasonable
	info, err := os.Stat(txtPath)
	require.NoError(t, err, "Failed to stat file")

	// Each code is ~54 chars + newline, so ~540KB total
	expectedSize := int64(10000 * 55)
	assert.InDelta(t, expectedSize, info.Size(), 1000, "File size should be around %d bytes", expectedSize)
}

func TestWriteTextFile_PreservesOrder(t *testing.T) {
	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "order.txt")

	codes := []string{"ZEBRA", "APPLE", "MANGO", "BANANA"}

	err := WriteTextFile(codes, txtPath)
	require.NoError(t, err, "WriteTextFile should not return error")

	content, err := os.ReadFile(txtPath)
	require.NoError(t, err, "Failed to read text file")

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	for i, expectedCode := range codes {
		if i < len(lines) {
			assert.Equal(t, expectedCode, lines[i], "Order should be preserved at position %d", i)
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
		require.NoError(b, err, "WriteTextFile should not return error")
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
		require.NoError(b, err, "WriteTextFile should not return error")
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
		require.NoError(b, err, "WriteTextFile should not return error")
	}
}

// TestWriteTextFile_UnicodeContent tests writing codes with unicode characters
func TestWriteTextFile_UnicodeContent(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	txtPath := filepath.Join(tmpDir, "unicode.txt")

	codes := []string{"CODE世界", "PROMO™️", "DEAL①②③"}

	err := WriteTextFile(codes, txtPath)
	require.NoError(t, err, "WriteTextFile should not return error")

	// Read back and verify
	content, err := os.ReadFile(txtPath)
	require.NoError(t, err, "Failed to read file")

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Len(t, lines, len(codes), "Expected %d lines", len(codes))

	for i, expectedCode := range codes {
		if i < len(lines) {
			assert.Equal(t, expectedCode, lines[i], "Unicode code mismatch at line %d", i+1)
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
	require.NoError(t, err, "WriteTextFile should not return error")

	content, err := os.ReadFile(txtPath)
	require.NoError(t, err, "Failed to read file")

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	// Verify all lines are preserved including empty ones
	assert.Len(t, lines, len(codes), "All lines should be preserved including empty ones")
}
