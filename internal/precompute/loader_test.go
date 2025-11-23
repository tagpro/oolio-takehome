package precompute

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadFile(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		content  string
		want     []string
		wantErr  bool
		setupErr bool // Whether to skip file creation to test errors
	}{
		{
			name: "valid file with multiple codes",
			content: `HAPPYHRS
FIFTYOFF
SUPER100
CODE12
AB`,
			want:    []string{"HAPPYHRS", "FIFTYOFF", "SUPER100", "CODE12", "AB"},
			wantErr: false,
		},
		{
			name:    "empty file",
			content: "",
			want:    nil,
			wantErr: false,
		},
		{
			name:    "file with only whitespace",
			content: "   \n\t\n   \n",
			want:    []string{"   ", "\t", "   "},
			wantErr: false,
		},
		{
			name: "file with empty lines",
			content: `CODE1

CODE2

CODE3`,
			want:    []string{"CODE1", "CODE2", "CODE3"},
			wantErr: false,
		},
		{
			name: "file with leading and trailing whitespace",
			content: `  CODE1
	CODE2
   CODE3   `,
			want:    []string{"  CODE1", "\tCODE2", "   CODE3   "},
			wantErr: false,
		},
		{
			name:    "single code",
			content: "SINGLECODE",
			want:    []string{"SINGLECODE"},
			wantErr: false,
		},
		{
			name:    "file with only newlines",
			content: "\n\n\n\n",
			want:    nil,
			wantErr: false,
		},
		{
			name: "file with mixed empty and non-empty lines",
			content: `

CODE1

CODE2


CODE3

`,
			want:    []string{"CODE1", "CODE2", "CODE3"},
			wantErr: false,
		},
		{
			name:     "non-existent file",
			setupErr: true,
			want:     nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var testFile string
			if tt.setupErr {
				// Use a non-existent file path
				testFile = filepath.Join(tmpDir, "nonexistent.txt")
			} else {
				// Create test file
				testFile = filepath.Join(tmpDir, tt.name+".txt")
				if err := os.WriteFile(testFile, []byte(tt.content), 0644); err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
			}

			got, err := LoadFile(testFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadFile_LargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "large.txt")

	// Create a large file with 10,000 codes
	var codes []string
	var builder strings.Builder
	for i := 0; i < 10000; i++ {
		code := "CODE" + strings.Repeat("X", 100) // 100 char codes
		codes = append(codes, code)
		builder.WriteString(code)
		builder.WriteString("\n")
	}

	if err := os.WriteFile(testFile, []byte(builder.String()), 0644); err != nil {
		t.Fatalf("Failed to create large test file: %v", err)
	}

	got, err := LoadFile(testFile)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}

	if len(got) != 10000 {
		t.Errorf("LoadFile() returned %d codes, want 10000", len(got))
	}
}

func TestLoadDirectory(t *testing.T) {
	t.Run("multiple files with shared codes", func(t *testing.T) {
		tmpDir := t.TempDir()

		// File 1
		file1 := filepath.Join(tmpDir, "codes1.txt")
		content1 := `HAPPYHRS
FIFTYOFF
TESTCODE1`
		if err := os.WriteFile(file1, []byte(content1), 0644); err != nil {
			t.Fatalf("Failed to create test file 1: %v", err)
		}

		// File 2
		file2 := filepath.Join(tmpDir, "codes2.txt")
		content2 := `HAPPYHRS
SUPER100
TESTCODE2`
		if err := os.WriteFile(file2, []byte(content2), 0644); err != nil {
			t.Fatalf("Failed to create test file 2: %v", err)
		}

		// File 3
		file3 := filepath.Join(tmpDir, "codes3.txt")
		content3 := `FIFTYOFF
SUPER100
TESTCODE3`
		if err := os.WriteFile(file3, []byte(content3), 0644); err != nil {
			t.Fatalf("Failed to create test file 3: %v", err)
		}

		codeToFiles, err := LoadDirectory(tmpDir)
		if err != nil {
			t.Fatalf("LoadDirectory() error = %v", err)
		}

		// Test that HAPPYHRS appears in files 0 and 1
		if files, ok := codeToFiles["HAPPYHRS"]; !ok {
			t.Error("HAPPYHRS not found in codeToFiles")
		} else if len(files) != 2 {
			t.Errorf("HAPPYHRS should appear in 2 files, got %d", len(files))
		}

		// Test that SUPER100 appears in files 1 and 2
		if files, ok := codeToFiles["SUPER100"]; !ok {
			t.Error("SUPER100 not found in codeToFiles")
		} else if len(files) != 2 {
			t.Errorf("SUPER100 should appear in 2 files, got %d", len(files))
		}

		// Test that TESTCODE1 appears in only 1 file
		if files, ok := codeToFiles["TESTCODE1"]; !ok {
			t.Error("TESTCODE1 not found in codeToFiles")
		} else if len(files) != 1 {
			t.Errorf("TESTCODE1 should appear in 1 file, got %d", len(files))
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		codeToFiles, err := LoadDirectory(tmpDir)
		if err != nil {
			t.Fatalf("LoadDirectory() error = %v", err)
		}

		if len(codeToFiles) != 0 {
			t.Errorf("Expected empty map, got %d codes", len(codeToFiles))
		}
	})

	t.Run("directory with subdirectories", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a file
		file1 := filepath.Join(tmpDir, "codes1.txt")
		if err := os.WriteFile(file1, []byte("CODE1\nCODE2"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Create a subdirectory (should be ignored)
		subDir := filepath.Join(tmpDir, "subdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}

		codeToFiles, err := LoadDirectory(tmpDir)
		if err != nil {
			t.Fatalf("LoadDirectory() error = %v", err)
		}

		// Should only have codes from file1
		if len(codeToFiles) != 2 {
			t.Errorf("Expected 2 codes, got %d", len(codeToFiles))
		}
	})

	t.Run("directory with single file", func(t *testing.T) {
		tmpDir := t.TempDir()

		file1 := filepath.Join(tmpDir, "single.txt")
		if err := os.WriteFile(file1, []byte("ONLY1\nONLY2"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		codeToFiles, err := LoadDirectory(tmpDir)
		if err != nil {
			t.Fatalf("LoadDirectory() error = %v", err)
		}

		if len(codeToFiles) != 2 {
			t.Errorf("Expected 2 codes, got %d", len(codeToFiles))
		}

		// Both codes should map to file index 0
		for code, files := range codeToFiles {
			if len(files) != 1 || files[0] != 0 {
				t.Errorf("Code %s should map to file index 0, got %v", code, files)
			}
		}
	})

	t.Run("non-existent directory", func(t *testing.T) {
		_, err := LoadDirectory("/path/that/does/not/exist")
		if err == nil {
			t.Error("Expected error for non-existent directory, got nil")
		}
	})

	t.Run("file indices are sequential", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create 5 files with a shared code
		for i := 0; i < 5; i++ {
			filename := filepath.Join(tmpDir, "file"+string(rune('0'+i))+".txt")
			if err := os.WriteFile(filename, []byte("SHARED"), 0644); err != nil {
				t.Fatalf("Failed to create test file %d: %v", i, err)
			}
		}

		codeToFiles, err := LoadDirectory(tmpDir)
		if err != nil {
			t.Fatalf("LoadDirectory() error = %v", err)
		}

		files := codeToFiles["SHARED"]
		if len(files) != 5 {
			t.Errorf("Expected 5 file indices, got %d", len(files))
		}

		// Verify indices are 0, 1, 2, 3, 4
		expected := []int{0, 1, 2, 3, 4}
		if !reflect.DeepEqual(files, expected) {
			t.Errorf("Expected file indices %v, got %v", expected, files)
		}
	})
}

func TestDeduplicateFileIndices(t *testing.T) {
	tests := []struct {
		name    string
		indices []int
		want    []int
	}{
		{
			name:    "no duplicates",
			indices: []int{0, 1, 2},
			want:    []int{0, 1, 2},
		},
		{
			name:    "with duplicates",
			indices: []int{0, 1, 1, 2, 0, 2},
			want:    []int{0, 1, 2},
		},
		{
			name:    "empty slice",
			indices: []int{},
			want:    []int{},
		},
		{
			name:    "nil input",
			indices: nil,
			want:    []int{},
		},
		{
			name:    "single element",
			indices: []int{5},
			want:    []int{5},
		},
		{
			name:    "all duplicates",
			indices: []int{7, 7, 7, 7, 7},
			want:    []int{7},
		},
		{
			name:    "preserves order",
			indices: []int{5, 2, 8, 2, 1, 5, 3},
			want:    []int{5, 2, 8, 1, 3},
		},
		{
			name:    "large indices",
			indices: []int{1000, 2000, 1000, 3000},
			want:    []int{1000, 2000, 3000},
		},
		{
			name:    "consecutive duplicates",
			indices: []int{1, 1, 2, 2, 3, 3},
			want:    []int{1, 2, 3},
		},
		{
			name:    "reverse order with duplicates",
			indices: []int{5, 4, 3, 4, 2, 1, 1},
			want:    []int{5, 4, 3, 2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeduplicateFileIndices(tt.indices)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeduplicateFileIndices() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Benchmarks

func BenchmarkLoadFile(b *testing.B) {
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "bench.txt")

	// Create a file with 1000 codes
	var builder strings.Builder
	for i := 0; i < 1000; i++ {
		builder.WriteString("CODE")
		builder.WriteString(strings.Repeat("X", 50))
		builder.WriteString("\n")
	}

	if err := os.WriteFile(testFile, []byte(builder.String()), 0644); err != nil {
		b.Fatalf("Failed to create benchmark file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadFile(testFile)
		if err != nil {
			b.Fatalf("LoadFile() error = %v", err)
		}
	}
}

func BenchmarkLoadDirectory(b *testing.B) {
	tmpDir := b.TempDir()

	// Create 10 files with 100 codes each
	for i := 0; i < 10; i++ {
		filename := filepath.Join(tmpDir, "codes"+strings.Repeat("0", 2-len(strings.Split(strings.Trim(strings.Repeat("0", i), "0"), "")))+".txt")
		var builder strings.Builder
		for j := 0; j < 100; j++ {
			builder.WriteString("CODE")
			builder.WriteString(strings.Repeat("X", 20))
			builder.WriteString("\n")
		}
		if err := os.WriteFile(filename, []byte(builder.String()), 0644); err != nil {
			b.Fatalf("Failed to create benchmark file: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadDirectory(tmpDir)
		if err != nil {
			b.Fatalf("LoadDirectory() error = %v", err)
		}
	}
}

func BenchmarkDeduplicateFileIndices(b *testing.B) {
	// Create a large slice with many duplicates
	indices := make([]int, 10000)
	for i := 0; i < 10000; i++ {
		indices[i] = i % 100 // Creates many duplicates
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DeduplicateFileIndices(indices)
	}
}

func BenchmarkDeduplicateFileIndices_NoDuplicates(b *testing.B) {
	// Create a large slice with no duplicates
	indices := make([]int, 10000)
	for i := 0; i < 10000; i++ {
		indices[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DeduplicateFileIndices(indices)
	}
}
