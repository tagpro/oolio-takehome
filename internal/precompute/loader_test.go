package precompute

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadFile(t *testing.T) {
	// Create a temporary test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_codes.txt")

	content := `HAPPYHRS
FIFTYOFF
SUPER100
CODE12
AB`

	err := os.WriteFile(testFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	tests := []struct {
		name     string
		filename string
		want     []string
		wantErr  bool
	}{
		{
			name:     "valid file",
			filename: testFile,
			want:     []string{"HAPPYHRS", "FIFTYOFF", "SUPER100", "CODE12", "AB"},
			wantErr:  false,
		},
		{
			name:     "non-existent file",
			filename: filepath.Join(tmpDir, "nonexistent.txt"),
			want:     nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadFile(tt.filename)
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

func TestLoadDirectory(t *testing.T) {
	// Create a temporary test directory with multiple files
	tmpDir := t.TempDir()

	// File 1
	file1 := filepath.Join(tmpDir, "codes1.txt")
	content1 := `HAPPYHRS
FIFTYOFF
TESTCODE1`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 1: %v", err)
	}

	// File 2
	file2 := filepath.Join(tmpDir, "codes2.txt")
	content2 := `HAPPYHRS
SUPER100
TESTCODE2`
	err = os.WriteFile(file2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 2: %v", err)
	}

	// File 3
	file3 := filepath.Join(tmpDir, "codes3.txt")
	content3 := `FIFTYOFF
SUPER100
TESTCODE3`
	err = os.WriteFile(file3, []byte(content3), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 3: %v", err)
	}

	codeToFiles, err := LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("LoadDirectory() error = %v", err)
	}

	// Test that HAPPYHRS appears in files 0 and 1 (codes1.txt and codes2.txt)
	if files, ok := codeToFiles["HAPPYHRS"]; !ok {
		t.Error("HAPPYHRS not found in codeToFiles")
	} else if len(files) < 2 {
		t.Errorf("HAPPYHRS should appear in at least 2 files, got %d", len(files))
	}

	// Test that SUPER100 appears in files 1 and 2 (codes2.txt and codes3.txt)
	if files, ok := codeToFiles["SUPER100"]; !ok {
		t.Error("SUPER100 not found in codeToFiles")
	} else if len(files) < 2 {
		t.Errorf("SUPER100 should appear in at least 2 files, got %d", len(files))
	}

	// Test that TESTCODE1 appears in only 1 file
	if files, ok := codeToFiles["TESTCODE1"]; !ok {
		t.Error("TESTCODE1 not found in codeToFiles")
	} else if len(files) != 1 {
		t.Errorf("TESTCODE1 should appear in 1 file, got %d", len(files))
	}
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
