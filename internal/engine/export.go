package engine

import (
	"github.com/ViMinhThang/LRdb/internal/memtable"
	"github.com/ViMinhThang/LRdb/internal/sstable"
)

// GetMemTable returns the current active memtable.
func (db *DB) GetMemTable() *memtable.SkipList {
	return db.memTable
}

// SetMemTable sets the current active memtable.
func (db *DB) SetMemTable(sl *memtable.SkipList) {
	db.memTable = sl
}

// GetImmutableMemTable returns the current immutable memtable.
func (db *DB) GetImmutableMemTable() *memtable.SkipList {
	return db.immutableMemtable
}

// SetImmutableMemTable sets the current immutable memtable.
func (db *DB) SetImmutableMemTable(sl *memtable.SkipList) {
	db.immutableMemtable = sl
}

// GetSSTables returns the current list of loaded SSTables.
func (db *DB) GetSSTables() []*sstable.SSTableReader {
	return db.sstables
}

// SetSSTables sets the list of loaded SSTables.
func (db *DB) SetSSTables(sstables []*sstable.SSTableReader) {
	db.sstables = sstables
}

// GetMaxLevel returns the maximum level configured for the memtables.
func (db *DB) GetMaxLevel() int {
	return db.maxLevel
}

// Lock locks the internal database mutex.
func (db *DB) Lock() {
	db.mu.Lock()
}

// Unlock unlocks the internal database mutex.
func (db *DB) Unlock() {
	db.mu.Unlock()
}
