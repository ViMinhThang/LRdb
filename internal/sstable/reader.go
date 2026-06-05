package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type SSTableReader struct {
	file          *os.File
	filePath      string
	indexManifest []IndexEntry
	mu            sync.Mutex
}

func NewSSTableReader(filePath string) (*SSTableReader, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTableReader{file: file, filePath: filePath}, nil
}

func (reader *SSTableReader) Close() error {
	return reader.file.Close()
}

func (reader *SSTableReader) Path() string {
	return reader.filePath
}

func OpenSSTableReader(filePath string) (*SSTableReader, error) {
	if _, err := os.Stat(filePath); err != nil {
		return nil, err
	}
	sstableReader, err := NewSSTableReader(filePath)
	if err != nil {
		return nil, err
	}
	manifest, err := sstableReader.ReadIndex()
	if err != nil {
		return nil, err
	}
	sstableReader.indexManifest = manifest
	return sstableReader, nil
}

func (reader *SSTableReader) ReadIndex() ([]IndexEntry, error) {
	_, err := reader.file.Seek(-16, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	footerBuf := make([]byte, 16)
	if _, err := reader.file.Read(footerBuf); err != nil {
		return nil, err
	}

	indexOffset := binary.BigEndian.Uint64(footerBuf[0:8])
	indexSize := binary.BigEndian.Uint64(footerBuf[8:16])

	if _, err := reader.file.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return nil, err
	}

	indexBuf := make([]byte, indexSize)

	if _, err := reader.file.Read(indexBuf); err != nil {
		return nil, err
	}

	var indexManifest []IndexEntry
	cursor := 0

	for cursor < len(indexBuf) {
		offset := binary.BigEndian.Uint64(indexBuf[cursor : cursor+8])
		size := binary.BigEndian.Uint64(indexBuf[cursor+8 : cursor+16])
		keySize := binary.BigEndian.Uint32(indexBuf[cursor+16 : cursor+20])
		cursor += 20

		lastKey := string(indexBuf[cursor : cursor+int(keySize)])
		cursor += int(keySize)

		indexManifest = append(indexManifest, IndexEntry{
			LastKey: lastKey,
			Offset:  offset,
			Size:    size,
		})
	}
	return indexManifest, nil
}
func (reader *SSTableReader) Get(key string) ([]byte, bool, error) {
	entry, found, err := reader.GetEntry(key)
	if err != nil || !found || entry.Deleted {
		return nil, false, err
	}
	return entry.Value, true, nil
}

func (reader *SSTableReader) GetEntry(key string) (Entry, bool, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()

	if len(reader.indexManifest) == 0 {
		return Entry{}, false, nil
	}

	left := 0
	right := len(reader.indexManifest) - 1
	for left < right {
		middleIndex := left + (right-left)/2
		middleValue := reader.indexManifest[middleIndex]
		if key <= middleValue.LastKey {
			right = middleIndex
		} else {
			left = middleIndex + 1
		}
	}
	if key > reader.indexManifest[left].LastKey {
		return Entry{}, false, nil
	}
	offset := reader.indexManifest[left].Offset
	if _, err := reader.file.Seek(int64(offset), io.SeekStart); err != nil {
		return Entry{}, false, err
	}

	blockSize := reader.indexManifest[left].Size
	blockBuf := make([]byte, blockSize)
	if _, err := io.ReadFull(reader.file, blockBuf); err != nil {
		return Entry{}, false, err
	}
	cursor := 0
	for cursor < len(blockBuf) {
		keySize := binary.BigEndian.Uint32(blockBuf[cursor : cursor+4])
		encodedValueSize := binary.BigEndian.Uint32(blockBuf[cursor+4 : cursor+8])
		deleted := encodedValueSize&tombstoneMask != 0
		valueSize := encodedValueSize &^ tombstoneMask

		cursor += 8

		currentKey := string(blockBuf[cursor : cursor+int(keySize)])
		cursor += int(keySize)

		value := blockBuf[cursor : cursor+int(valueSize)]
		cursor += int(valueSize)

		if currentKey == key {
			return Entry{Key: currentKey, Value: value, Deleted: deleted}, true, nil
		}
		if currentKey > key {
			break
		}
	}
	return Entry{}, false, nil
}

func (reader *SSTableReader) Entries() ([]Entry, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()

	var entries []Entry
	for _, indexEntry := range reader.indexManifest {
		if _, err := reader.file.Seek(int64(indexEntry.Offset), io.SeekStart); err != nil {
			return nil, err
		}

		blockBuf := make([]byte, indexEntry.Size)
		if _, err := io.ReadFull(reader.file, blockBuf); err != nil {
			return nil, err
		}

		blockEntries, err := parseBlockEntries(blockBuf)
		if err != nil {
			return nil, err
		}
		entries = append(entries, blockEntries...)
	}
	return entries, nil
}

func parseBlockEntries(blockBuf []byte) ([]Entry, error) {
	var entries []Entry
	cursor := 0
	for cursor < len(blockBuf) {
		keySize := binary.BigEndian.Uint32(blockBuf[cursor : cursor+4])
		encodedValueSize := binary.BigEndian.Uint32(blockBuf[cursor+4 : cursor+8])
		deleted := encodedValueSize&tombstoneMask != 0
		valueSize := encodedValueSize &^ tombstoneMask

		cursor += 8

		key := string(blockBuf[cursor : cursor+int(keySize)])
		cursor += int(keySize)

		value := blockBuf[cursor : cursor+int(valueSize)]
		cursor += int(valueSize)

		entries = append(entries, Entry{Key: key, Value: value, Deleted: deleted})
	}
	return entries, nil
}
