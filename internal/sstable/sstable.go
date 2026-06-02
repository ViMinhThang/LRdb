package sstable

import (
	"os"
)

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
