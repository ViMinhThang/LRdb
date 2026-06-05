package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
)

const tombstoneMask uint32 = 1 << 31

type Entry struct {
	Key     string
	Value   []byte
	Deleted bool
}

type IndexEntry struct {
	LastKey string
	Offset  uint64
	Size    uint64
}

type SSTableBuilder struct {
	file              *os.File
	filePath          string
	currentBlockSize  uint64
	currentFileOffset uint64
	indexManifest     []IndexEntry
	blockSizeLimit    uint64
}

func NewSSTableBuilder(filePath string, blockSizeLimit uint64) (*SSTableBuilder, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTableBuilder{
		file:           file,
		filePath:       filePath,
		blockSizeLimit: blockSizeLimit,
	}, nil
}

func (b *SSTableBuilder) Append(key string, value []byte) error {
	return b.AppendEntry(Entry{Key: key, Value: value})
}

func (b *SSTableBuilder) AppendTombstone(key string) error {
	return b.AppendEntry(Entry{Key: key, Deleted: true})
}

func (b *SSTableBuilder) AppendEntry(entry Entry) error {
	if len(entry.Value) > int(tombstoneMask-1) {
		return fmt.Errorf("value size exceeds limit")
	}

	keybuf := []byte(entry.Key)
	keySize := uint32(len(keybuf))
	valueSize := uint32(len(entry.Value))

	if b.currentBlockSize == 0 {
		newEntry := IndexEntry{
			Offset: b.currentFileOffset,
		}
		b.indexManifest = append(b.indexManifest, newEntry)
	}

	kvSize := uint64(8 + keySize + valueSize) // 4 bye keySize + 4 bytes valueSize + Data

	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], keySize)
	encodedValueSize := valueSize
	if entry.Deleted {
		encodedValueSize |= tombstoneMask
	}
	binary.BigEndian.PutUint32(header[4:8], encodedValueSize)

	if _, err := b.file.Write(header); err != nil {
		return err
	}

	if _, err := b.file.Write(keybuf); err != nil {
		return err
	}

	if _, err := b.file.Write(entry.Value); err != nil {
		return err
	}

	currentIndex := len(b.indexManifest) - 1
	b.indexManifest[currentIndex].LastKey = entry.Key
	b.indexManifest[currentIndex].Size += kvSize

	b.currentFileOffset += kvSize
	b.currentBlockSize += kvSize

	if b.currentBlockSize >= b.blockSizeLimit {
		b.currentBlockSize = 0
	}
	return nil
}
func (b *SSTableBuilder) Finish() error {
	indexOffset := b.currentFileOffset

	for _, entry := range b.indexManifest {
		keybuf := []byte(entry.LastKey)
		keySize := uint32(len(keybuf))

		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[0:8], entry.Offset)
		binary.BigEndian.PutUint64(buf[8:16], entry.Size)
		binary.BigEndian.PutUint32(buf[16:20], keySize)

		if _, err := b.file.Write(buf); err != nil {
			return err
		}

		if _, err := b.file.Write(keybuf); err != nil {
			return err
		}
		b.currentFileOffset += uint64(20 + keySize)
	}
	indexSize := b.currentFileOffset - indexOffset

	footer := make([]byte, 16)
	binary.BigEndian.PutUint64(footer[0:8], indexOffset)
	binary.BigEndian.PutUint64(footer[8:16], indexSize)
	if _, err := b.file.Write(footer); err != nil {
		return err
	}
	return b.file.Close()
}
