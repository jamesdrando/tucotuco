package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLogAppendRoundTripAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	first := Record{
		Type:     RecordTypePageImage,
		Resource: "public_widgets.heap",
		PageID:   0,
		Payload:  bytes.Repeat([]byte{0x11}, 256),
	}
	second := Record{
		Type:     RecordTypePageImage,
		Resource: "public_widgets.heap",
		PageID:   7,
		Payload:  bytes.Repeat([]byte{0x7f}, 256),
	}

	firstLSN, err := log.Append(first)
	if err != nil {
		t.Fatalf("append first record: %v", err)
	}
	secondLSN, err := log.Append(second)
	if err != nil {
		t.Fatalf("append second record: %v", err)
	}
	if firstLSN == 0 || secondLSN <= firstLSN {
		t.Fatalf("unexpected lsn ordering: %d then %d", firstLSN, secondLSN)
	}
	if err := log.Sync(secondLSN); err != nil {
		t.Fatalf("sync wal: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() {
		_ = reopened.Close()
	}()

	records, err := reopened.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want 2", len(records))
	}
	if records[0].LSN != firstLSN || records[1].LSN != secondLSN {
		t.Fatalf("reopened lsns = %#v", records)
	}
	if records[0].Resource != first.Resource || records[1].PageID != second.PageID {
		t.Fatalf("reopened records = %#v", records)
	}
	if !bytes.Equal(records[0].Payload, first.Payload) || !bytes.Equal(records[1].Payload, second.Payload) {
		t.Fatal("payload round-trip mismatch")
	}
	if reopened.DurableLSN() != secondLSN {
		t.Fatalf("durable lsn = %d, want %d", reopened.DurableLSN(), secondLSN)
	}
}

func TestOpenTruncatesPartialTailRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	firstLSN, err := log.Append(Record{
		Type:     RecordTypePageImage,
		Resource: "public_items.heap",
		PageID:   1,
		Payload:  bytes.Repeat([]byte{0x44}, 128),
	})
	if err != nil {
		t.Fatalf("append first record: %v", err)
	}
	secondLSN, err := log.Append(Record{
		Type:     RecordTypePageImage,
		Resource: "public_items.heap",
		PageID:   2,
		Payload:  bytes.Repeat([]byte{0x99}, 128),
	})
	if err != nil {
		t.Fatalf("append second record: %v", err)
	}
	if err := log.Sync(secondLSN); err != nil {
		t.Fatalf("sync wal: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	truncatedSize := info.Size() - 9
	if err := os.Truncate(path, truncatedSize); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() {
		_ = reopened.Close()
	}()

	records, err := reopened.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1", len(records))
	}
	if records[0].LSN != firstLSN {
		t.Fatalf("surviving record lsn = %d, want %d", records[0].LSN, firstLSN)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat truncated wal: %v", err)
	}
	if info.Size() >= truncatedSize {
		t.Fatalf("tail was not truncated on reopen: size=%d truncated=%d", info.Size(), truncatedSize)
	}
}

func TestOpenRejectsCorruptRecordChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	lsn, err := log.Append(Record{
		Type:     RecordTypePageImage,
		Resource: "public_items.heap",
		PageID:   1,
		Payload:  bytes.Repeat([]byte{0x55}, 128),
	})
	if err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := log.Sync(lsn); err != nil {
		t.Fatalf("sync wal: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open raw wal: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(fileHeaderSize+recordHeaderSize+1)); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt wal payload: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw wal: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("expected reopen failure for corrupt wal")
	}
}
