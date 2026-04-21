package wal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	// ErrClosed reports that the WAL handle is already closed.
	ErrClosed = errors.New("wal: log is closed")
	// ErrInvalidRecord reports a malformed append request or persisted record.
	ErrInvalidRecord = errors.New("wal: invalid record")
)

// Log is a file-backed write-ahead log.
type Log struct {
	mu         sync.Mutex
	file       *os.File
	nextLSN    LSN
	lastLSN    LSN
	durableLSN LSN
	closed     bool
}

// Open opens or creates the WAL file at path.
func Open(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	log := &Log{file: file}
	if err := log.bootstrap(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return log, nil
}

// Append appends record to the WAL and returns its assigned LSN.
func (l *Log) Append(record Record) (LSN, error) {
	return l.AppendWith(func(LSN) (Record, error) {
		return record, nil
	})
}

// AppendWith appends a record built for the assigned LSN.
func (l *Log) AppendWith(builder RecordBuilder) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureOpen(); err != nil {
		return 0, err
	}
	if builder == nil {
		return 0, ErrInvalidRecord
	}

	record, err := builder(l.nextLSN)
	if err != nil {
		return 0, err
	}
	header, resource, payload, err := l.prepareRecordLocked(record)
	if err != nil {
		return 0, err
	}

	header.Checksum = 0
	headerBuf := encodeRecordHeader(header)
	header.Checksum = computeRecordChecksum(headerBuf, resource, payload)
	headerBuf = encodeRecordHeader(header)

	offset := int64(header.LSN)
	if _, err := l.file.WriteAt(headerBuf, offset); err != nil {
		return 0, err
	}
	if len(resource) > 0 {
		if _, err := l.file.WriteAt(resource, offset+recordHeaderSize); err != nil {
			return 0, err
		}
	}
	if len(payload) > 0 {
		if _, err := l.file.WriteAt(payload, offset+recordHeaderSize+int64(len(resource))); err != nil {
			return 0, err
		}
	}

	l.lastLSN = header.LSN
	l.nextLSN = header.LSN + LSN(recordSize(header))
	return header.LSN, nil
}

// Sync fsyncs the WAL file through target.
func (l *Log) Sync(target LSN) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureOpen(); err != nil {
		return err
	}
	if target == 0 || target <= l.durableLSN {
		return nil
	}
	if l.lastLSN == 0 || target > l.lastLSN {
		return fmt.Errorf("wal: sync target %d exceeds last record %d", target, l.lastLSN)
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.durableLSN = l.lastLSN
	return nil
}

// DurableLSN reports the highest fsynced WAL LSN.
func (l *Log) DurableLSN() LSN {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.durableLSN
}

// Records decodes every persisted WAL record in order.
func (l *Log) Records() ([]PersistedRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureOpen(); err != nil {
		return nil, err
	}

	var records []PersistedRecord
	_, _, err := l.scanLocked(false, func(record PersistedRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Close closes the WAL file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	return l.file.Close()
}

func (l *Log) bootstrap() error {
	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		header := encodeFileHeader(fileHeader{
			Magic:      fileMagic,
			Version:    fileVersion,
			HeaderSize: fileHeaderSize,
		})
		if _, err := l.file.WriteAt(header, 0); err != nil {
			return err
		}
		if err := l.file.Sync(); err != nil {
			return err
		}
		l.nextLSN = fileHeaderSize
		return nil
	}

	headerBuf := make([]byte, fileHeaderSize)
	if _, err := l.file.ReadAt(headerBuf, 0); err != nil {
		return err
	}
	if _, err := decodeFileHeader(headerBuf); err != nil {
		return err
	}

	lastLSN, nextLSN, err := l.scanLocked(true, nil)
	if err != nil {
		return err
	}
	l.lastLSN = lastLSN
	l.nextLSN = nextLSN
	l.durableLSN = lastLSN
	return nil
}

func (l *Log) scanLocked(
	truncateTail bool,
	visitor func(PersistedRecord) error,
) (lastLSN LSN, nextLSN LSN, err error) {
	info, err := l.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()
	offset := int64(fileHeaderSize)
	nextLSN = fileHeaderSize

	for offset < size {
		if size-offset < recordHeaderSize {
			if truncateTail {
				if err := l.file.Truncate(offset); err != nil {
					return 0, 0, err
				}
				return lastLSN, LSN(offset), nil
			}
			return 0, 0, io.ErrUnexpectedEOF
		}

		headerBuf := make([]byte, recordHeaderSize)
		if _, err := l.file.ReadAt(headerBuf, offset); err != nil {
			return 0, 0, err
		}
		header, err := decodeRecordHeader(headerBuf)
		if err != nil {
			return 0, 0, err
		}
		if header.LSN != LSN(offset) {
			return 0, 0, fmt.Errorf("wal: record lsn %d does not match offset %d", header.LSN, offset)
		}

		totalSize := recordSize(header)
		if totalSize < recordHeaderSize {
			return 0, 0, ErrInvalidRecord
		}
		if offset+totalSize > size {
			if truncateTail {
				if err := l.file.Truncate(offset); err != nil {
					return 0, 0, err
				}
				return lastLSN, LSN(offset), nil
			}
			return 0, 0, io.ErrUnexpectedEOF
		}

		resource := make([]byte, int(header.ResourceLen))
		if len(resource) > 0 {
			if _, err := l.file.ReadAt(resource, offset+recordHeaderSize); err != nil {
				return 0, 0, err
			}
		}
		payload := make([]byte, int(header.PayloadLen))
		if len(payload) > 0 {
			if _, err := l.file.ReadAt(payload, offset+recordHeaderSize+int64(len(resource))); err != nil {
				return 0, 0, err
			}
		}
		if computeRecordChecksum(zeroChecksumHeader(headerBuf), resource, payload) != header.Checksum {
			return 0, 0, fmt.Errorf("wal: checksum mismatch at lsn %d", header.LSN)
		}

		record := PersistedRecord{
			LSN:      header.LSN,
			Type:     header.Type,
			Resource: string(resource),
			PageID:   header.PageID,
			Payload:  payload,
		}
		if visitor != nil {
			if err := visitor(record); err != nil {
				return 0, 0, err
			}
		}

		lastLSN = record.LSN
		offset += totalSize
		nextLSN = LSN(offset)
	}

	return lastLSN, nextLSN, nil
}

func (l *Log) prepareRecordLocked(record Record) (recordHeader, []byte, []byte, error) {
	if record.Type == 0 || record.Resource == "" || len(record.Payload) == 0 {
		return recordHeader{}, nil, nil, ErrInvalidRecord
	}

	resource := []byte(record.Resource)
	if len(resource) > int(^uint32(0)) || len(record.Payload) > int(^uint32(0)) {
		return recordHeader{}, nil, nil, ErrInvalidRecord
	}

	payload := append([]byte(nil), record.Payload...)
	header := recordHeader{
		Magic:       recordMagic,
		Version:     recordVersion,
		Type:        record.Type,
		LSN:         l.nextLSN,
		PageID:      record.PageID,
		ResourceLen: uint32(len(resource)),
		PayloadLen:  uint32(len(payload)),
	}
	return header, resource, payload, nil
}

func (l *Log) ensureOpen() error {
	if l.closed {
		return ErrClosed
	}
	return nil
}

func computeRecordChecksum(header, resource, payload []byte) uint32 {
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(header)
	_, _ = checksum.Write(resource)
	_, _ = checksum.Write(payload)
	return checksum.Sum32()
}

func zeroChecksumHeader(header []byte) []byte {
	clone := append([]byte(nil), header...)
	for index := 0x20; index < 0x24 && index < len(clone); index++ {
		clone[index] = 0
	}
	return clone
}
