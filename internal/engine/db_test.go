package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/memtable"
)

func openTestDB(t *testing.T) (*DB, string, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "lrdb-engine-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	walPath := filepath.Join(tmpDir, "test.wal")
	db, err := OpenDB(walPath, 8)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return db, tmpDir, walPath
}

func flushActive(t *testing.T, db *DB) {
	t.Helper()

	db.mu.Lock()
	if db.immutableMemtable != nil {
		db.mu.Unlock()
		t.Fatal("immutable memtable is already pending flush")
	}
	if db.memTable.Size() == 0 {
		db.mu.Unlock()
		t.Fatal("active memtable is empty")
	}
	db.immutableMemtable = db.memTable
	db.memTable = memtable.NewSkipList(db.maxLevel)
	db.mu.Unlock()

	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush active memtable: %v", err)
	}
}

func TestDB_DeleteVisibleBeforeAndAfterFlush(t *testing.T) {
	db, _, _ := openTestDB(t)

	if err := db.Put("gone", []byte("value")); err != nil {
		t.Fatalf("failed to put value: %v", err)
	}
	if err := db.Delete("gone"); err != nil {
		t.Fatalf("failed to delete value: %v", err)
	}

	if val, found := db.Get("gone"); found || val != nil {
		t.Fatalf("expected active tombstone to hide key, got value=%q found=%v", val, found)
	}

	flushActive(t, db)

	if val, found := db.Get("gone"); found || val != nil {
		t.Fatalf("expected flushed tombstone to hide key, got value=%q found=%v", val, found)
	}
}

func TestDB_CompactionKeepsNewestValueAndCleansOldFiles(t *testing.T) {
	db, tmpDir, _ := openTestDB(t)

	if err := db.Put("key", []byte("value-1")); err != nil {
		t.Fatalf("failed to put first value: %v", err)
	}
	flushActive(t, db)

	if err := db.Put("key", []byte("value-2")); err != nil {
		t.Fatalf("failed to put second value: %v", err)
	}
	flushActive(t, db)

	if err := db.Put("alpha", []byte("first")); err != nil {
		t.Fatalf("failed to put alpha: %v", err)
	}
	flushActive(t, db)

	if err := db.Put("omega", []byte("last")); err != nil {
		t.Fatalf("failed to put omega: %v", err)
	}
	flushActive(t, db)

	if len(db.sstables) != 1 {
		t.Fatalf("expected compaction to leave 1 SSTable, got %d", len(db.sstables))
	}

	value, found := db.Get("key")
	if !found || !bytes.Equal(value, []byte("value-2")) {
		t.Fatalf("expected newest value, got value=%q found=%v", value, found)
	}

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("failed to list SSTables: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected old SSTable files to be cleaned up, got %d files: %v", len(files), files)
	}
}

func TestDB_CompactionDropsTombstonesAndRestartKeepsDelete(t *testing.T) {
	db, _, walPath := openTestDB(t)

	if err := db.Put("gone", []byte("old")); err != nil {
		t.Fatalf("failed to put gone: %v", err)
	}
	flushActive(t, db)

	if err := db.Delete("gone"); err != nil {
		t.Fatalf("failed to delete gone: %v", err)
	}
	flushActive(t, db)

	if err := db.Put("live-1", []byte("one")); err != nil {
		t.Fatalf("failed to put live-1: %v", err)
	}
	flushActive(t, db)

	if err := db.Put("live-2", []byte("two")); err != nil {
		t.Fatalf("failed to put live-2: %v", err)
	}
	flushActive(t, db)

	if len(db.sstables) != 1 {
		t.Fatalf("expected compaction to leave 1 SSTable, got %d", len(db.sstables))
	}
	if val, found := db.Get("gone"); found || val != nil {
		t.Fatalf("expected deleted key to stay hidden after compaction, got value=%q found=%v", val, found)
	}

	entries, err := db.sstables[0].Entries()
	if err != nil {
		t.Fatalf("failed to scan compacted SSTable: %v", err)
	}
	for _, entry := range entries {
		if entry.Key == "gone" {
			t.Fatalf("expected compaction to drop final tombstone, found %+v", entry)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	reopened, err := OpenDB(walPath, 8)
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer reopened.Close()

	if val, found := reopened.Get("gone"); found || val != nil {
		t.Fatalf("expected WAL tombstone to hide key after restart, got value=%q found=%v", val, found)
	}
	value, found := reopened.Get("live-1")
	if !found || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("expected live value after restart, got value=%q found=%v", value, found)
	}
}

func TestDB_ConcurrentWritesRecoverInMemoryState(t *testing.T) {
	db, _, walPath := openTestDB(t)

	const (
		keyCount   = 8
		writeCount = 80
	)

	start := make(chan struct{})
	errs := make(chan error, writeCount)
	var wg sync.WaitGroup

	for i := 0; i < writeCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			key := fmt.Sprintf("key-%02d", i%keyCount)
			if i%5 == 0 {
				errs <- db.Delete(key)
				return
			}
			errs <- db.Put(key, []byte(fmt.Sprintf("value-%02d", i)))
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}

	expected := make(map[string][]byte)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value, found := db.Get(key)
		if found {
			expected[key] = append([]byte(nil), value...)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	reopened, err := OpenDB(walPath, 8)
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer reopened.Close()

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key-%02d", i)
		expectedValue, expectedFound := expected[key]
		actualValue, actualFound := reopened.Get(key)

		if actualFound != expectedFound {
			t.Fatalf("key %s: expected found=%v, got found=%v value=%q", key, expectedFound, actualFound, actualValue)
		}
		if expectedFound && !bytes.Equal(actualValue, expectedValue) {
			t.Fatalf("key %s: expected value=%q, got %q", key, expectedValue, actualValue)
		}
	}
}
