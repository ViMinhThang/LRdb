package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/ViMinhThang/LRdb/internal/memtable"
	"github.com/ViMinhThang/LRdb/internal/sstable"
	"github.com/ViMinhThang/LRdb/internal/wal"
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
			mem.Put(rec.Key, rec.Value)
		}
	}

	dbDir := filepath.Dir(walPath)
	files, err := os.ReadDir(dbDir)
	var sstables []*sstable.SSTableReader
	var nextSSTableID uint64 = 0

	if err == nil {
		var sstFiles []string
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".sst" {
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
		memTableSizeLimit: 4 * 1024 * 1024, // 4MB default limit
	}, nil
}

func (db *DB) Put(Key string, value []byte) error {
	if err := db.wal.Write(Key, value); err != nil {
		return err
	}

	db.mu.Lock()
	db.memTable.Put(Key, value)

	var triggerFlush bool
	if db.memTable.Size() >= db.memTableSizeLimit && db.immutableMemtable == nil {
		db.immutableMemtable = db.memTable
		db.memTable = memtable.NewSkipList(db.maxLevel)
		triggerFlush = true
	}
	db.mu.Unlock()

	if triggerFlush {
		go func() {
			if err := db.Flush(); err != nil {
				// Handle flush error (e.g. logging or panic in production)
			}
		}()
	}

	return nil
}
func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 1. Search active memtable
	if value, found := db.memTable.Get(key); found {
		return value, true
	}

	// 2. Search immutable memtable
	if db.immutableMemtable != nil {
		if value, found := db.immutableMemtable.Get(key); found {
			return value, true
		}
	}

	// 3. Search SSTables on disk (newest to oldest)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		if value, found, err := db.sstables[i].Get(key); err == nil && found {
			return value, true
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

	// Determine the new SSTable file name (e.g. 00001.sst)
	sstPath := filepath.Join(filepath.Dir(db.walPath), fmt.Sprintf("%05d.sst", db.nextSSTableID))
	db.nextSSTableID++
	db.mu.Unlock()

	// Create a new SSTable builder
	builder, err := sstable.NewSSTableBuilder(sstPath, 4096) // 4KB block size limit
	if err != nil {
		return err
	}

	iter := db.immutableMemtable.NewIterator()
	for iter.Next() {
		if err := builder.Append(iter.Key(), iter.Value()); err != nil {
			return err
		}
		iter.Advance()
	}

	if err := builder.Finish(); err != nil {
		return err
	}

	// Open the new SSTable for reading
	reader, err := sstable.OpenSSTableReader(sstPath)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.sstables = append(db.sstables, reader)
	db.immutableMemtable = nil // Flush complete, clear memory
	db.mu.Unlock()

	return nil
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
