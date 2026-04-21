package wal

import (
	"encoding/binary"
	"fmt"
)

const (
	fileMagic        uint32 = 0x4c415754 // "TWAL" in little-endian form.
	fileVersion      uint16 = 1
	fileHeaderSize          = 16
	recordMagic      uint32 = 0x524c4157 // "WALR" in little-endian form.
	recordVersion    uint16 = 1
	recordHeaderSize        = 40
)

// LSN identifies one WAL record position inside the log stream.
type LSN uint64

// RecordType identifies the durable WAL payload family.
type RecordType uint16

const (
	// RecordTypePageImage stores one full-page image for redo.
	RecordTypePageImage RecordType = 1
)

// Record describes one WAL append request.
type Record struct {
	Type     RecordType
	Resource string
	PageID   uint64
	Payload  []byte
}

// RecordBuilder materializes a WAL record for the supplied append LSN.
type RecordBuilder func(lsn LSN) (Record, error)

// PersistedRecord is one fully decoded WAL record.
type PersistedRecord struct {
	LSN      LSN
	Type     RecordType
	Resource string
	PageID   uint64
	Payload  []byte
}

type fileHeader struct {
	Magic      uint32
	Version    uint16
	Reserved0  uint16
	HeaderSize uint32
	Reserved1  uint32
}

type recordHeader struct {
	Magic       uint32
	Version     uint16
	Type        RecordType
	LSN         LSN
	PageID      uint64
	ResourceLen uint32
	PayloadLen  uint32
	Checksum    uint32
	Reserved    uint32
}

func encodeFileHeader(header fileHeader) []byte {
	buf := make([]byte, fileHeaderSize)
	binary.LittleEndian.PutUint32(buf[0x00:0x04], header.Magic)
	binary.LittleEndian.PutUint16(buf[0x04:0x06], header.Version)
	binary.LittleEndian.PutUint16(buf[0x06:0x08], header.Reserved0)
	binary.LittleEndian.PutUint32(buf[0x08:0x0c], header.HeaderSize)
	binary.LittleEndian.PutUint32(buf[0x0c:0x10], header.Reserved1)
	return buf
}

func decodeFileHeader(buf []byte) (fileHeader, error) {
	if len(buf) != fileHeaderSize {
		return fileHeader{}, fmt.Errorf("wal: file header size %d, want %d", len(buf), fileHeaderSize)
	}

	header := fileHeader{
		Magic:      binary.LittleEndian.Uint32(buf[0x00:0x04]),
		Version:    binary.LittleEndian.Uint16(buf[0x04:0x06]),
		Reserved0:  binary.LittleEndian.Uint16(buf[0x06:0x08]),
		HeaderSize: binary.LittleEndian.Uint32(buf[0x08:0x0c]),
		Reserved1:  binary.LittleEndian.Uint32(buf[0x0c:0x10]),
	}
	if header.Magic != fileMagic {
		return fileHeader{}, fmt.Errorf("wal: invalid file magic 0x%x", header.Magic)
	}
	if header.Version != fileVersion {
		return fileHeader{}, fmt.Errorf("wal: unsupported file version %d", header.Version)
	}
	if header.HeaderSize != fileHeaderSize {
		return fileHeader{}, fmt.Errorf("wal: invalid file header size %d", header.HeaderSize)
	}
	return header, nil
}

func encodeRecordHeader(header recordHeader) []byte {
	buf := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint32(buf[0x00:0x04], header.Magic)
	binary.LittleEndian.PutUint16(buf[0x04:0x06], header.Version)
	binary.LittleEndian.PutUint16(buf[0x06:0x08], uint16(header.Type))
	binary.LittleEndian.PutUint64(buf[0x08:0x10], uint64(header.LSN))
	binary.LittleEndian.PutUint64(buf[0x10:0x18], header.PageID)
	binary.LittleEndian.PutUint32(buf[0x18:0x1c], header.ResourceLen)
	binary.LittleEndian.PutUint32(buf[0x1c:0x20], header.PayloadLen)
	binary.LittleEndian.PutUint32(buf[0x20:0x24], header.Checksum)
	binary.LittleEndian.PutUint32(buf[0x24:0x28], header.Reserved)
	return buf
}

func decodeRecordHeader(buf []byte) (recordHeader, error) {
	if len(buf) != recordHeaderSize {
		return recordHeader{}, fmt.Errorf("wal: record header size %d, want %d", len(buf), recordHeaderSize)
	}

	header := recordHeader{
		Magic:       binary.LittleEndian.Uint32(buf[0x00:0x04]),
		Version:     binary.LittleEndian.Uint16(buf[0x04:0x06]),
		Type:        RecordType(binary.LittleEndian.Uint16(buf[0x06:0x08])),
		LSN:         LSN(binary.LittleEndian.Uint64(buf[0x08:0x10])),
		PageID:      binary.LittleEndian.Uint64(buf[0x10:0x18]),
		ResourceLen: binary.LittleEndian.Uint32(buf[0x18:0x1c]),
		PayloadLen:  binary.LittleEndian.Uint32(buf[0x1c:0x20]),
		Checksum:    binary.LittleEndian.Uint32(buf[0x20:0x24]),
		Reserved:    binary.LittleEndian.Uint32(buf[0x24:0x28]),
	}
	if header.Magic != recordMagic {
		return recordHeader{}, fmt.Errorf("wal: invalid record magic 0x%x", header.Magic)
	}
	if header.Version != recordVersion {
		return recordHeader{}, fmt.Errorf("wal: unsupported record version %d", header.Version)
	}
	if header.Type == 0 {
		return recordHeader{}, fmt.Errorf("wal: invalid record type %d", header.Type)
	}
	return header, nil
}

func recordSize(header recordHeader) int64 {
	return int64(recordHeaderSize) + int64(header.ResourceLen) + int64(header.PayloadLen)
}
