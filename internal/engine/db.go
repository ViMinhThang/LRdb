package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ViMinhThang/LRdb/internal/memtable"
	"github.com/ViMinhThang/LRdb/internal/sstable"
	"github.com/ViMinhThang/LRdb/internal/wal"
)

const (
	defaultMemTableSizeLimit = 4 * 1024 * 1024
	defaultSSTableBlockSize  = 4096
	compactionThreshold      = 4
)

type DB struct {
	memTable          *memtable.SkipList
	immutableMemtable *memtable.SkipList
	wal               *wal.WAL
	walPath           string
	nextSSTableID     uint64
	sstables          []*sstable.SSTableReader
	maxLevel          int
	memTableSizeLimit int64
	compactionRunning bool
	mu                sync.RWMutex
}

func OpenDB(walPath string, maxLevel int) (*DB, error) {
	mem := memtable.NewSkipList(maxLevel)
	if _, err := os.Stat(walPath); err == nil {
		file, err := wal.OpenWALForRead(walPath)
		if err != nil {
			return nil, err
		}
		records, err := wal.ReadRecords(file)
		file.Close()
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			if rec.Deleted {
				mem.Delete(rec.Key)
			} else {
				mem.Put(rec.Key, rec.Value)
			}
		}
	}

	dbDir := filepath.Dir(walPath)
	files, err := os.ReadDir(dbDir)
	var sstables []*sstable.SSTableReader
	var nextSSTableID uint64 = 0

	if err == nil {
		var sstFiles []string
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			if strings.HasSuffix(file.Name(), ".sst.tmp") {
				_ = os.Remove(filepath.Join(dbDir, file.Name()))
				continue
			}
			if filepath.Ext(file.Name()) == ".sst" {
				sstFiles = append(sstFiles, file.Name())
			}
		}
		sort.Strings(sstFiles)

		for _, sstFile := range sstFiles {
			var sstID uint64
			if _, errScan := fmt.Sscanf(sstFile, "%d.sst", &sstID); errScan == nil {
				if sstID >= nextSSTableID {
					nextSSTableID = sstID + 1
				}
			}

			fullPath := filepath.Join(dbDir, sstFile)
			reader, errOpen := sstable.OpenSSTableReader(fullPath)
			if errOpen != nil {
				return nil, errOpen
			}
			sstables = append(sstables, reader)
		}
	}

	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		memTable:          mem,
		wal:               w,
		walPath:           walPath,
		nextSSTableID:     nextSSTableID,
		sstables:          sstables,
		maxLevel:          maxLevel,
		memTableSizeLimit: defaultMemTableSizeLimit,
	}, nil
}

func (db *DB) Put(key string, value []byte) error {
	db.mu.Lock()
	if err := db.wal.Write(key, value); err != nil {
		db.mu.Unlock()
		return err
	}

	db.memTable.Put(key, value)
	triggerFlush := db.rotateMemtableIfNeededLocked()
	db.mu.Unlock()

	if triggerFlush {
		db.flushAsync()
	}

	return nil
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	if err := db.wal.WriteDelete(key); err != nil {
		db.mu.Unlock()
		return err
	}

	db.memTable.Delete(key)
	triggerFlush := db.rotateMemtableIfNeededLocked()
	db.mu.Unlock()

	if triggerFlush {
		db.flushAsync()
	}

	return nil
}

func (db *DB) rotateMemtableIfNeededLocked() bool {
	if db.memTable.Size() < db.memTableSizeLimit || db.immutableMemtable != nil {
		return false
	}

	db.immutableMemtable = db.memTable
	db.memTable = memtable.NewSkipList(db.maxLevel)
	return true
}

func (db *DB) flushAsync() {
	go func() {
		if err := db.Flush(); err != nil {
			// Handle flush error (e.g. logging or panic in production)
		}
	}()
}

func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 1. Search active memtable
	if entry, found := db.memTable.GetEntry(key); found {
		if entry.Deleted {
			return nil, false
		}
		return entry.Value, true
	}

	// 2. Search immutable memtable
	if db.immutableMemtable != nil {
		if entry, found := db.immutableMemtable.GetEntry(key); found {
			if entry.Deleted {
				return nil, false
			}
			return entry.Value, true
		}
	}

	// 3. Search SSTables on disk (newest to oldest)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		if entry, found, err := db.sstables[i].GetEntry(key); err == nil && found {
			if entry.Deleted {
				return nil, false
			}
			return entry.Value, true
		}
	}

	return nil, false
}
func (db *DB) Flush() error {
	db.mu.Lock()
	if db.immutableMemtable == nil {
		db.mu.Unlock()
		return nil
	}
	memToFlush := db.immutableMemtable

	// Determine the new SSTable file name (e.g. 00001.sst)
	sstPath := filepath.Join(filepath.Dir(db.walPath), fmt.Sprintf("%05d.sst", db.nextSSTableID))
	db.nextSSTableID++
	db.mu.Unlock()

	var entries []sstable.Entry
	iter := memToFlush.NewIterator()
	for iter.Next() {
		entry := iter.Entry()
		entries = append(entries, sstable.Entry{Key: entry.Key, Value: entry.Value, Deleted: entry.Deleted})
		iter.Advance()
	}

	reader, err := writeSSTableAtomically(sstPath, entries)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.sstables = append(db.sstables, reader)
	if db.immutableMemtable == memToFlush {
		db.immutableMemtable = nil // Flush complete, clear memory
	}
	db.mu.Unlock()

	return db.maybeCompact()
}

func (db *DB) maybeCompact() error {
	db.mu.Lock()
	if len(db.sstables) < compactionThreshold || db.compactionRunning {
		db.mu.Unlock()
		return nil
	}

	readers := append([]*sstable.SSTableReader(nil), db.sstables...)
	sstPath := filepath.Join(filepath.Dir(db.walPath), fmt.Sprintf("%05d.sst", db.nextSSTableID))
	db.nextSSTableID++
	db.compactionRunning = true
	db.mu.Unlock()

	defer func() {
		db.mu.Lock()
		db.compactionRunning = false
		db.mu.Unlock()
	}()

	return db.compactSSTables(readers, sstPath)
}

func (db *DB) compactSSTables(readers []*sstable.SSTableReader, sstPath string) error {
	latest := make(map[string]sstable.Entry)
	for _, reader := range readers {
		entries, err := reader.Entries()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			value := entry.Value
			if !entry.Deleted {
				value = append([]byte(nil), entry.Value...)
			}
			latest[entry.Key] = sstable.Entry{Key: entry.Key, Value: value, Deleted: entry.Deleted}
		}
	}

	var keys []string
	for key, entry := range latest {
		if !entry.Deleted {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var newReader *sstable.SSTableReader
	if len(keys) > 0 {
		reader, err := writeCompactedSSTable(sstPath, keys, latest)
		if err != nil {
			_ = os.Remove(sstPath)
			return err
		}
		newReader = reader
	}

	compacted := make(map[*sstable.SSTableReader]struct{}, len(readers))
	for _, reader := range readers {
		compacted[reader] = struct{}{}
	}

	db.mu.Lock()
	remaining := make([]*sstable.SSTableReader, 0, len(db.sstables))
	for _, reader := range db.sstables {
		if _, ok := compacted[reader]; !ok {
			remaining = append(remaining, reader)
		}
	}

	nextSSTables := make([]*sstable.SSTableReader, 0, len(remaining)+1)
	if newReader != nil {
		nextSSTables = append(nextSSTables, newReader)
	}
	nextSSTables = append(nextSSTables, remaining...)
	db.sstables = nextSSTables
	db.mu.Unlock()

	var firstErr error
	for _, reader := range readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(reader.Path()); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func writeCompactedSSTable(sstPath string, keys []string, entries map[string]sstable.Entry) (*sstable.SSTableReader, error) {
	compactedEntries := make([]sstable.Entry, 0, len(keys))
	for _, key := range keys {
		compactedEntries = append(compactedEntries, entries[key])
	}
	return writeSSTableAtomically(sstPath, compactedEntries)
}

func writeSSTableAtomically(sstPath string, entries []sstable.Entry) (*sstable.SSTableReader, error) {
	tmpPath := sstPath + ".tmp"
	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	builder, err := sstable.NewSSTableBuilder(tmpPath, defaultSSTableBlockSize)
	if err != nil {
		return nil, err
	}

	builderClosed := false
	cleanupTemp := true
	defer func() {
		if !builderClosed {
			_ = builder.Close()
		}
		if cleanupTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	for _, entry := range entries {
		if err := builder.AppendEntry(entry); err != nil {
			return nil, err
		}
	}
	if err := builder.Finish(); err != nil {
		return nil, err
	}
	builderClosed = true

	if err := os.Rename(tmpPath, sstPath); err != nil {
		return nil, err
	}
	cleanupTemp = false

	return sstable.OpenSSTableReader(sstPath)
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.Close(); err != nil {
		return err
	}
	for _, reader := range db.sstables {
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}
