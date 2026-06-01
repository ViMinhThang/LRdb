package engine

import (
	"os"

	"github.com/ViMinhThang/LRdb/internal/memtable"
	"github.com/ViMinhThang/LRdb/internal/wal"
)

type DB struct {
	memTable *memtable.SkipList
	wal      *wal.WAL
	walPath  string
}

func OpenDB(walPath string, maxLevel int) (*DB, error) {
	mem := memtable.NewSkipList(maxLevel)
	if _, err := os.Stat(walPath); err != nil {
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
	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		memTable: mem,
		wal:      w,
		walPath:  walPath,
	}, nil
}

func (db *DB) Put(Key string, value []byte) error {
	if err := db.wal.Write(Key, value); err != nil {
		return err
	}
	db.memTable.Put(Key, value)
	return nil
}
func (db *DB) Get(key string) ([]byte, bool) {
	return db.memTable.Get(key)
}

func (db *DB) Close() error {
	return db.wal.Close()
}
