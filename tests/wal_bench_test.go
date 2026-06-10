package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/wal"
)

func BenchmarkWAL_Write(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "bench.wal")
	w, err := wal.NewWAL(walPath)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := w.Write(key, value); err != nil {
			b.Fatalf("Failed to write to WAL: %v", err)
		}
	}
}
