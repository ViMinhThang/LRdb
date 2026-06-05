package sstable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "test.sst")

	// 1. Create SSTable with small block size (50 bytes) to force multiple blocks
	builder, err := NewSSTableBuilder(filePath, 50)
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
	reader, err := OpenSSTableReader(filePath)
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
		"apricot",   // Before "apple"
		"blueberry", // Between blocks
		"grape",     // After "fig"
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

func TestSSTable_TombstonesAndEntries(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "test.sst")

	builder, err := NewSSTableBuilder(filePath, 50)
	if err != nil {
		t.Fatalf("failed to create builder: %v", err)
	}

	if err := builder.Append("apple", []byte("red")); err != nil {
		t.Fatalf("failed to append apple: %v", err)
	}
	if err := builder.AppendTombstone("banana"); err != nil {
		t.Fatalf("failed to append banana tombstone: %v", err)
	}
	if err := builder.Append("cherry", []byte{}); err != nil {
		t.Fatalf("failed to append cherry: %v", err)
	}
	if err := builder.Finish(); err != nil {
		t.Fatalf("failed to finish builder: %v", err)
	}

	reader, err := OpenSSTableReader(filePath)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	if val, found, err := reader.Get("banana"); err != nil || found || val != nil {
		t.Fatalf("expected tombstone to be hidden, got value=%q found=%v err=%v", val, found, err)
	}

	entry, found, err := reader.GetEntry("banana")
	if err != nil {
		t.Fatalf("failed to get tombstone entry: %v", err)
	}
	if !found || !entry.Deleted || entry.Key != "banana" || len(entry.Value) != 0 {
		t.Fatalf("expected banana tombstone entry, got entry=%+v found=%v", entry, found)
	}

	val, found, err := reader.Get("cherry")
	if err != nil {
		t.Fatalf("failed to get live empty value: %v", err)
	}
	if !found || !bytes.Equal(val, []byte{}) {
		t.Fatalf("expected live empty value, got value=%q found=%v", val, found)
	}

	entries, err := reader.Entries()
	if err != nil {
		t.Fatalf("failed to scan entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	expectedKeys := []string{"apple", "banana", "cherry"}
	for i, key := range expectedKeys {
		if entries[i].Key != key {
			t.Fatalf("expected entry %d key %s, got %s", i, key, entries[i].Key)
		}
	}
	if entries[0].Deleted || !entries[1].Deleted || entries[2].Deleted {
		t.Fatalf("unexpected deletion flags: %+v", entries)
	}
}
