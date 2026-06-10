package tests

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/sstable"
)

func BenchmarkSSTable_Build(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "sstable_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filePath := filepath.Join(tempDir, fmt.Sprintf("bench_%d.sst", i))
		builder, err := sstable.NewSSTableBuilder(filePath, 4096)
		if err != nil {
			b.Fatalf("Failed to create builder: %v", err)
		}
		b.StartTimer()

		for k := 0; k < 1000; k++ {
			key := fmt.Sprintf("key-%08d", k)
			if err := builder.Append(key, value); err != nil {
				b.Fatalf("Failed to append: %v", err)
			}
		}

		if err := builder.Finish(); err != nil {
			b.Fatalf("Failed to finish builder: %v", err)
		}
	}
}

func BenchmarkSSTable_Get(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "sstable_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "bench_get.sst")
	builder, err := sstable.NewSSTableBuilder(filePath, 4096)
	if err != nil {
		b.Fatalf("Failed to create builder: %v", err)
	}

	const numKeys = 10000
	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("key-%08d", k)
		if err := builder.Append(key, value); err != nil {
			b.Fatalf("Failed to append: %v", err)
		}
	}
	if err := builder.Finish(); err != nil {
		b.Fatalf("Failed to finish builder: %v", err)
	}

	reader, err := sstable.OpenSSTableReader(filePath)
	if err != nil {
		b.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", rand.Intn(numKeys))
		_, _, _ = reader.Get(key)
	}
}
