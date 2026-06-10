package tests

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ViMinhThang/LRdb/internal/engine"
)

func newBenchDB(b *testing.B) (*engine.DB, string) {
	tmpDir, err := os.MkdirTemp("", "db_bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	walPath := filepath.Join(tmpDir, "bench.wal")
	db, err := engine.OpenDB(walPath, 8)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to open db: %v", err)
	}

	return db, tmpDir
}

func BenchmarkDB_Put(b *testing.B) {
	db, tmpDir := newBenchDB(b)
	defer func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}()

	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("failed to put: %v", err)
		}
	}
}

func BenchmarkDB_GetActiveMemTable(b *testing.B) {
	db, tmpDir := newBenchDB(b)
	defer func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}()

	const numKeys = 1000
	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		_ = db.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", rand.Intn(numKeys))
		_, _ = db.Get(key)
	}
}

func BenchmarkDB_GetSSTable(b *testing.B) {
	db, tmpDir := newBenchDB(b)
	defer func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}()

	const numKeys = 50000 // Large enough to trigger rotation and flush to SSTables
	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		_ = db.Put(key, value)
	}

	// Wait for any async flushes to finish
	db.Lock()
	for db.GetImmutableMemTable() != nil {
		db.Unlock()
		time.Sleep(10 * time.Millisecond)
		db.Lock()
	}
	db.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", rand.Intn(numKeys))
		_, _ = db.Get(key)
	}
}

func BenchmarkDB_ReadWriteMix(b *testing.B) {
	db, tmpDir := newBenchDB(b)
	defer func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}()

	const numKeys = 10000
	value := []byte("value-data-1234567890-abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		_ = db.Put(key, value)
	}

	// We run multiple goroutines to simulate mixed concurrent read/write access
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(rand.Uint64())))
		for pb.Next() {
			key := fmt.Sprintf("key-%08d", r.Intn(numKeys))
			if r.Float64() < 0.1 { // 10% writes
				_ = db.Put(key, value)
			} else { // 90% reads
				_, _ = db.Get(key)
			}
		}
	})
}
