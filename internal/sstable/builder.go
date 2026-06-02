package sstable

import (
	"encoding/binary"
	"os"
)

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
	keybuf := []byte(key)
	keySize := uint32(len(keybuf))
	valueSize := uint32(len(value))

	if b.currentBlockSize == 0 {
		newEntry := IndexEntry{
			Offset: b.currentFileOffset,
		}
		b.indexManifest = append(b.indexManifest, newEntry)
	}

	kvSize := uint64(8 + keySize + valueSize) // 4 bye keySize + 4 bytes valueSize + Data

	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], keySize)
	binary.BigEndian.PutUint32(header[4:8], valueSize)

	if _, err := b.file.Write(header); err != nil {
		return err
	}

	if _, err := b.file.Write(keybuf); err != nil {
		return err
	}

	if _, err := b.file.Write(value); err != nil {
		return err
	}

	currentIndex := len(b.indexManifest) - 1
	b.indexManifest[currentIndex].LastKey = key
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
