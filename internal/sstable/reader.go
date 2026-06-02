package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/ViMinhThang/LRdb/internal/sstable"
)
type SSTableReader struct {
	file     *os.File
	filePath string
}

func NewSSTableReader(filePath string) (*SSTableReader, error) {

}

func OpenSSTableReader(filePath string) error {
	if _, err := os.Stat(filePath); err != nil {
		return err
	}
	sstableReader, err := sstable.NewSSTableReader(filePath)
	if err != nil {
		return err
	}
	indexTable,err := sstableReader.
}

func (reader *SSTableReader) SSTableReader() ([]IndexEntry, error) {
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
