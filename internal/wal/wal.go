package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type WAL struct {
	file *os.File
}

const maxRecordsSize = 64 * 1024 * 1024
const tombstoneMask uint32 = 1 << 31

type Record struct {
	Key     string
	Value   []byte
	Deleted bool
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (w *WAL) Write(key string, value []byte) error {
	return w.writeRecord(key, value, false)
}

func (w *WAL) WriteDelete(key string) error {
	return w.writeRecord(key, nil, true)
}

func (w *WAL) writeRecord(key string, value []byte, deleted bool) error {
	keyBuf := []byte(key)
	keySize := len(keyBuf)
	valueSize := len(value)
	if valueSize > int(tombstoneMask-1) {
		return fmt.Errorf("value size exceeds limit")
	}

	recordSize := 12 + keySize + valueSize
	record := make([]byte, recordSize)

	binary.BigEndian.PutUint32(record[4:8], uint32(keySize))
	encodedValueSize := uint32(valueSize)
	if deleted {
		encodedValueSize |= tombstoneMask
	}
	binary.BigEndian.PutUint32(record[8:12], encodedValueSize)

	copy(record[12:12+keySize], keyBuf)
	copy(record[12+keySize:], value)

	checksum := crc32.ChecksumIEEE(record[4:])

	binary.BigEndian.PutUint32(record[0:4], checksum)

	if _, err := w.file.Write(record); err != nil {
		return err
	}

	return w.file.Sync()
}

func OpenWALForRead(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0644)
}

func ReadRecords(file *os.File) ([]Record, error) {
	var records []Record
	header := make([]byte, 12)
	for {
		_, err := io.ReadFull(file, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		expectedChecksum := binary.BigEndian.Uint32(header[0:4])
		keySize := binary.BigEndian.Uint32(header[4:8])
		encodedValueSize := binary.BigEndian.Uint32(header[8:12])
		deleted := encodedValueSize&tombstoneMask != 0
		valueSize := encodedValueSize &^ tombstoneMask

		if uint64(keySize)+uint64(valueSize) > maxRecordsSize {
			return records, fmt.Errorf("record size exceeds limit")
		}

		dataBuf := make([]byte, keySize+valueSize)

		if _, err := io.ReadFull(file, dataBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		hashser := crc32.NewIEEE()
		hashser.Write(header[4:12])
		hashser.Write(dataBuf)
		actualChecksum := hashser.Sum32()
		if actualChecksum != expectedChecksum {
			return records, fmt.Errorf("corruption detected at record %d", len(records))
		}
		key := string(dataBuf[0:keySize])
		value := dataBuf[keySize:]

		records = append(records, Record{Key: key, Value: value, Deleted: deleted})
	}
	return records, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
