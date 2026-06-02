package sstable

import "encoding/binary"

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
