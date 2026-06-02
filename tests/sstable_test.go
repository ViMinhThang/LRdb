package tests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/sstable"
)

func TestSSTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "test.sst")

	// 1. Create SSTable with small block size (50 bytes) to force multiple blocks
	builder, err := sstable.NewSSTableBuilder(filePath, 50)
	if err != nil {
		t.Fatalf("failed to create builder: %v", err)
	}

	data := []struct {
		key   string
		value string
	}{
		{"apple", "red-fruit"},
		{"banana", "yellow-fruit"},
		{"cherry", "red-berry"},
		{"date", "sweet-fruit"},
		{"elderberry", "purple-berry"},
		{"fig", "sweet-purple-fruit"},
	}

	for _, d := range data {
		if err := builder.Append(d.key, []byte(d.value)); err != nil {
			t.Fatalf("failed to append %s: %v", d.key, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("failed to finish builder: %v", err)
	}

	// 2. Read the SSTable using SSTableReader
	reader, err := sstable.OpenSSTableReader(filePath)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	// 3. Test retrieving existing keys
	for _, d := range data {
		val, found, err := reader.Get(d.key)
		if err != nil {
			t.Errorf("error getting key %s: %v", d.key, err)
		}
		if !found {
			t.Errorf("expected to find key %s, but got not found", d.key)
		}
		if !bytes.Equal(val, []byte(d.value)) {
			t.Errorf("expected value %s for key %s, got %s", d.value, d.key, string(val))
		}
	}

	// 4. Test retrieving non-existent keys
	nonExistent := []string{
		"apricot", // Before "apple"
		"blueberry", // Between blocks
		"grape", // After "fig"
	}

	for _, k := range nonExistent {
		val, found, err := reader.Get(k)
		if err != nil {
			t.Errorf("error getting non-existent key %s: %v", k, err)
		}
		if found {
			t.Errorf("expected key %s to not be found, but got value %s", k, string(val))
		}
	}
}
