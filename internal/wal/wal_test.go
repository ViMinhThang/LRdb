package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_WriteAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")

	// 1. Create WAL and write records
	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	recordsToWrite := []Record{
		{Key: "key-1", Value: []byte("value-1")},
		{Key: "key-2", Value: []byte("value-2")},
		{Key: "key-3", Value: []byte("")}, // empty value
		{Key: "", Value: []byte("value-4")},  // empty key
	}

	for _, r := range recordsToWrite {
		if err := w.Write(r.Key, r.Value); err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// 2. Open for read and verify records
	file, err := OpenWALForRead(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL for read: %v", err)
	}
	defer file.Close()

	readRecords, err := ReadRecords(file)
	if err != nil {
		t.Fatalf("Failed to read WAL records: %v", err)
	}

	if len(readRecords) != len(recordsToWrite) {
		t.Fatalf("Expected %d records, got %d", len(recordsToWrite), len(readRecords))
	}

	for i, r := range recordsToWrite {
		if readRecords[i].Key != r.Key {
			t.Errorf("Record %d: expected key %q, got %q", i, r.Key, readRecords[i].Key)
		}
		if !bytes.Equal(readRecords[i].Value, r.Value) {
			t.Errorf("Record %d: expected value %q, got %q", i, r.Value, readRecords[i].Value)
		}
	}
}

func TestWAL_AppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")

	// Write first record
	w1, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL 1: %v", err)
	}
	if err := w1.Write("first", []byte("data1")); err != nil {
		t.Fatalf("Failed to write first record: %v", err)
	}
	w1.Close()

	// Append second record
	w2, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL 2: %v", err)
	}
	if err := w2.Write("second", []byte("data2")); err != nil {
		t.Fatalf("Failed to write second record: %v", err)
	}
	w2.Close()

	// Read and verify both
	file, err := OpenWALForRead(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL for read: %v", err)
	}
	defer file.Close()

	records, err := ReadRecords(file)
	if err != nil {
		t.Fatalf("Failed to read WAL: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}
	if records[0].Key != "first" || !bytes.Equal(records[0].Value, []byte("data1")) {
		t.Errorf("Unexpected first record: %+v", records[0])
	}
	if records[1].Key != "second" || !bytes.Equal(records[1].Value, []byte("data2")) {
		t.Errorf("Unexpected second record: %+v", records[1])
	}
}

func TestWAL_EmptyWAL(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "empty.wal")

	// Create and close an empty file
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}
	f.Close()

	file, err := OpenWALForRead(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL for read: %v", err)
	}
	defer file.Close()

	records, err := ReadRecords(file)
	if err != nil {
		t.Fatalf("Failed to read empty WAL: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from empty WAL, got %d", len(records))
	}
}

func TestWAL_CorruptionAndTruncation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "corrupted.wal")

	// Write three records
	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	w.Write("key1", []byte("val1"))
	w.Write("key2", []byte("val2"))
	w.Write("key3", []byte("val3"))
	w.Close()

	// Read raw content to corrupt the second record
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read raw WAL file: %v", err)
	}

	// Record size for "key1" / "val1" is 12 + 4 + 4 = 20 bytes
	// Second record starts at offset 20.
	// Let's modify a byte of the second record's payload (e.g. at offset 35) to invalidate the checksum
	corruptedData := make([]byte, len(data))
	copy(corruptedData, data)
	if len(corruptedData) > 35 {
		corruptedData[35] ^= 0xFF // Flip bits to cause checksum failure
	}

	corruptedPath := filepath.Join(tempDir, "bad.wal")
	if err := os.WriteFile(corruptedPath, corruptedData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted WAL: %v", err)
	}

	// Verify that the corrupted record returns an error, but successfully returns the first healthy record
	file, err := OpenWALForRead(corruptedPath)
	if err != nil {
		t.Fatalf("Failed to open bad WAL: %v", err)
	}
	defer file.Close()

	records, err := ReadRecords(file)
	if err == nil {
		t.Fatalf("Expected corruption error, but got nil error")
	}

	// Since record 2 has a bad checksum, we should get exactly 1 record ("key1") returned alongside the error.
	if len(records) != 1 {
		t.Errorf("Expected 1 record before corruption, got %d", len(records))
	} else if records[0].Key != "key1" {
		t.Errorf("Expected key1, got %q", records[0].Key)
	}

	// Let's test truncation (cut the file off inside record 3)
	// First record is 20 bytes. Let's truncate at 30 bytes (in the middle of second record).
	truncatedData := data[:30]
	truncatedPath := filepath.Join(tempDir, "truncated.wal")
	if err := os.WriteFile(truncatedPath, truncatedData, 0644); err != nil {
		t.Fatalf("Failed to write truncated WAL: %v", err)
	}

	fileTrunc, err := OpenWALForRead(truncatedPath)
	if err != nil {
		t.Fatalf("Failed to open truncated WAL: %v", err)
	}
	defer fileTrunc.Close()

	recordsTrunc, err := ReadRecords(fileTrunc)
	if err != nil {
		t.Fatalf("Failed to parse truncated WAL: %v", err)
	}

	// Should successfully read record 1, and gracefully stop on record 2 due to hitting EOF
	if len(recordsTrunc) != 1 {
		t.Errorf("Expected 1 record from truncated WAL, got %d", len(recordsTrunc))
	} else if recordsTrunc[0].Key != "key1" {
		t.Errorf("Expected key1, got %q", recordsTrunc[0].Key)
	}
}
