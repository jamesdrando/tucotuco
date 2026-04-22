package paged

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

func TestRelationVacuumReclaimsDeletedTupleSpaceAndPreservesCommittedScan(t *testing.T) {
	root := t.TempDir()
	manager, relation := openVacuumTestRelation(t, root, "vacuum_delete", 512)
	defer func() {
		_ = manager.Close()
	}()

	keepHandle, err := relation.Insert(pagedTestRow(1, "keep-a"))
	if err != nil {
		t.Fatalf("insert kept row: %v", err)
	}
	deleteHandle, err := relation.Insert(pagedTestRow(2, strings.Repeat("d", 48)))
	if err != nil {
		t.Fatalf("insert deleted row: %v", err)
	}
	secondKeepHandle, err := relation.Insert(pagedTestRow(3, "keep-b"))
	if err != nil {
		t.Fatalf("insert second kept row: %v", err)
	}

	if PageID(keepHandle.Page) != 1 || PageID(deleteHandle.Page) != 1 || PageID(secondKeepHandle.Page) != 1 {
		t.Fatalf(
			"delete-compaction fixture handles = %#v %#v %#v, want all on page 1",
			keepHandle,
			deleteHandle,
			secondKeepHandle,
		)
	}

	if err := relation.Delete(deleteHandle); err != nil {
		t.Fatalf("delete row: %v", err)
	}

	beforeRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, beforeRecords, []storage.Record{
		{Handle: keepHandle, Row: pagedTestRow(1, "keep-a")},
		{Handle: secondKeepHandle, Row: pagedTestRow(3, "keep-b")},
	})
	if _, err := relation.Lookup(deleteHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted row before vacuum error = %v, want %v", err, ErrRowNotFound)
	}

	beforePage := readVacuumPageState(t, relation, 1)
	if beforePage.header.DeadBytes == 0 {
		t.Fatalf("page 1 dead bytes before vacuum = %d, want > 0", beforePage.header.DeadBytes)
	}

	invokeRelationVacuum(t, relation)

	afterRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, afterRecords, beforeRecords)
	if _, err := relation.Lookup(deleteHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted row after vacuum error = %v, want %v", err, ErrRowNotFound)
	}

	afterPage := readVacuumPageState(t, relation, 1)
	if afterPage.header.DeadBytes != 0 {
		t.Fatalf("page 1 dead bytes after vacuum = %d, want 0", afterPage.header.DeadBytes)
	}
	if afterPage.freeSpace <= beforePage.freeSpace {
		t.Fatalf(
			"page 1 free space after vacuum = %d, want > before vacuum %d",
			afterPage.freeSpace,
			beforePage.freeSpace,
		)
	}
}

func TestRelationVacuumPreservesRedirectRootLookupAndCommittedScan(t *testing.T) {
	root := t.TempDir()
	manager, relation := openVacuumTestRelation(t, root, "vacuum_redirects", 256)
	defer func() {
		_ = manager.Close()
	}()

	fixture := prepareVacuumRedirectFixture(t, relation)

	beforeRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, beforeRecords, fixture.committedRecords)

	beforeIntermediate := readHeapPageTupleSnapshot(t, relation, PageID(fixture.firstReplacement.Page), fixture.firstReplacement.Slot)
	assertTupleVersionState(t, "pre-vacuum intermediate version", beforeIntermediate, tupleVersionExpectation{
		allowedSlotFlags: []uint16{slotFlagLive, slotFlagDead},
		xmin:             expectPresent,
		xmax:             expectPresent,
		forwardPtr:       expectPresent,
		deletedFlag:      expectEither,
	})
	beforeIntermediatePage := readVacuumPageState(t, relation, PageID(fixture.firstReplacement.Page))

	got, err := relation.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("lookup redirected root before vacuum: %v", err)
	}
	assertStorageRowEqual(t, got, fixture.latestRow)

	invokeRelationVacuum(t, relation)

	afterRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, afterRecords, beforeRecords)

	got, err = relation.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("lookup redirected root after vacuum: %v", err)
	}
	assertStorageRowEqual(t, got, fixture.latestRow)

	if afterRecords[0].Handle != fixture.rootHandle {
		t.Fatalf("scan handle[0] after vacuum = %#v, want root handle %#v", afterRecords[0].Handle, fixture.rootHandle)
	}

	afterIntermediatePage := readVacuumPageState(t, relation, PageID(fixture.firstReplacement.Page))
	if afterIntermediatePage.freeSpace <= beforeIntermediatePage.freeSpace {
		t.Fatalf(
			"page %d free space after vacuum = %d, want > before vacuum %d",
			fixture.firstReplacement.Page,
			afterIntermediatePage.freeSpace,
			beforeIntermediatePage.freeSpace,
		)
	}
}

func TestRelationTxRemainsAlignedWithCommittedStateAcrossVacuum(t *testing.T) {
	root := t.TempDir()
	manager, relation := openVacuumTestRelation(t, root, "vacuum_tx_alignment", 256)
	defer func() {
		_ = manager.Close()
	}()

	fixture := prepareVacuumRedirectFixture(t, relation)

	tx, err := relation.BeginTransaction(storage.TransactionOptions{Isolation: storage.IsolationSerializable})
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	beforeCommitted := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, beforeCommitted, fixture.committedRecords)
	beforeTx := collectPagedRecords(t, mustScanTx(t, tx, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, beforeTx, beforeCommitted)

	invokeRelationVacuum(t, relation)

	afterCommitted := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, afterCommitted, beforeCommitted)
	afterTx := collectPagedRecords(t, mustScanTx(t, tx, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, afterTx, afterCommitted)

	got, err := tx.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("tx lookup redirected root after vacuum: %v", err)
	}
	assertStorageRowEqual(t, got, fixture.latestRow)

	if _, err := tx.Lookup(fixture.deletedHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("tx lookup deleted row after vacuum error = %v, want %v", err, ErrRowNotFound)
	}

	staged := pagedTestRow(1, strings.Repeat("s", 80))
	if err := tx.Update(fixture.rootHandle, staged); err != nil {
		t.Fatalf("tx update redirected root after vacuum: %v", err)
	}

	stagedLookup, err := tx.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("tx lookup staged row: %v", err)
	}
	assertStorageRowEqual(t, stagedLookup, staged)

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after vacuum: %v", err)
	}

	committed, err := relation.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("lookup committed staged row: %v", err)
	}
	assertStorageRowEqual(t, committed, staged)
}

func TestRelationVacuumDurabilityAcrossReopenAndRecovery(t *testing.T) {
	root := t.TempDir()
	desc := pagedTestTableDescriptor("vacuum_recovery")

	manager, relation := openVacuumTestRelationWithDescriptor(t, root, desc, 256)
	fixture := prepareVacuumRedirectFixture(t, relation)

	beforeVacuumMetadata, err := relation.Metadata()
	if err != nil {
		_ = manager.Close()
		t.Fatalf("metadata before vacuum: %v", err)
	}
	beforeVacuumRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, beforeVacuumRecords, fixture.committedRecords)

	invokeRelationVacuum(t, relation)

	afterVacuumRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, afterVacuumRecords, beforeVacuumRecords)

	pageIDs := uniquePageIDs(
		0,
		PageID(fixture.rootHandle.Page),
		PageID(fixture.deletedHandle.Page),
		PageID(fixture.firstReplacement.Page),
		PageID(fixture.secondReplacement.Page),
	)
	latestImages := selectRelationPageImages(t, latestRelationPageImages(t, root, desc), pageIDs...)

	if err := manager.Close(); err != nil {
		t.Fatalf("close source manager: %v", err)
	}

	overwriteRelationPageImages(t, root, desc, 256, zeroedRelationPageImages(256, pageIDs...))

	reopenedManager, reopenedRelation := openVacuumTestRelationWithDescriptor(t, root, desc, 256)
	defer func() {
		_ = reopenedManager.Close()
	}()

	reopenedRow, err := reopenedRelation.Lookup(fixture.rootHandle)
	if err != nil {
		t.Fatalf("lookup redirected root after recovery: %v", err)
	}
	assertStorageRowEqual(t, reopenedRow, fixture.latestRow)

	if _, err := reopenedRelation.Lookup(fixture.deletedHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted row after recovery error = %v, want %v", err, ErrRowNotFound)
	}

	reopenedRecords := collectPagedRecords(t, mustScanRelation(t, reopenedRelation, storage.ScanOptions{}))
	assertPagedRecordsEqual(t, reopenedRecords, afterVacuumRecords)
	assertRelationPageImagesEqual(t, root, desc, 256, pageIDs, latestImages)

	reopenedMetadata, err := reopenedRelation.Metadata()
	if err != nil {
		t.Fatalf("metadata after recovery: %v", err)
	}
	if reopenedMetadata.NextVersion < beforeVacuumMetadata.NextVersion {
		t.Fatalf(
			"metadata next version after recovery = %d, want >= pre-vacuum %d",
			reopenedMetadata.NextVersion,
			beforeVacuumMetadata.NextVersion,
		)
	}

	postVacuumHandle, err := reopenedRelation.Insert(pagedTestRow(99, "post-vacuum"))
	if err != nil {
		t.Fatalf("insert after recovery: %v", err)
	}
	postVacuumSnapshot := readHeapPageTupleSnapshot(t, reopenedRelation, PageID(postVacuumHandle.Page), postVacuumHandle.Slot)
	if postVacuumSnapshot.header.Xmin < beforeVacuumMetadata.NextVersion {
		t.Fatalf(
			"post-recovery insert xmin = %d, want >= pre-vacuum next version %d",
			postVacuumSnapshot.header.Xmin,
			beforeVacuumMetadata.NextVersion,
		)
	}

	if err := reopenedManager.Close(); err != nil {
		t.Fatalf("close reopened manager: %v", err)
	}
}

type vacuumRedirectFixture struct {
	rootHandle        storage.RowHandle
	deletedHandle     storage.RowHandle
	firstReplacement  storage.RowHandle
	secondReplacement storage.RowHandle
	latestRow         storage.Row
	committedRecords  []storage.Record
}

type vacuumPageState struct {
	header    PageHeader
	freeSpace int
}

func prepareVacuumRedirectFixture(t *testing.T, relation *Relation) vacuumRedirectFixture {
	t.Helper()

	rootHandle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert root row: %v", err)
	}
	fillerHandle, err := relation.Insert(pagedTestRow(2, "filler"))
	if err != nil {
		t.Fatalf("insert filler row: %v", err)
	}

	firstUpdate := pagedTestRow(1, strings.Repeat("x", 96))
	if err := relation.Update(rootHandle, firstUpdate); err != nil {
		t.Fatalf("first redirecting update: %v", err)
	}
	firstReplacement := assertHeapPageRedirect(t, relation, PageID(rootHandle.Page), rootHandle.Slot)

	latestRow := pagedTestRow(1, strings.Repeat("y", 120))
	if err := relation.Update(rootHandle, latestRow); err != nil {
		t.Fatalf("second redirecting update: %v", err)
	}
	secondReplacement := assertHeapPageRedirect(t, relation, PageID(rootHandle.Page), rootHandle.Slot)

	if firstReplacement == secondReplacement {
		t.Fatalf("redirect replacement handles match: %#v", firstReplacement)
	}
	if firstReplacement.Page == secondReplacement.Page {
		t.Fatalf("redirect replacement pages = %d and %d, want distinct pages for vacuum fixture", firstReplacement.Page, secondReplacement.Page)
	}

	deletedHandle, err := relation.Insert(pagedTestRow(3, "doomed"))
	if err != nil {
		t.Fatalf("insert deleted row: %v", err)
	}
	if err := relation.Delete(deletedHandle); err != nil {
		t.Fatalf("delete doomed row: %v", err)
	}
	if _, err := relation.Lookup(deletedHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted fixture row error = %v, want %v", err, ErrRowNotFound)
	}

	committedRecords := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	wantRecords := []storage.Record{
		{Handle: rootHandle, Row: latestRow},
		{Handle: fillerHandle, Row: pagedTestRow(2, "filler")},
	}
	assertPagedRecordsEqual(t, committedRecords, wantRecords)

	return vacuumRedirectFixture{
		rootHandle:        rootHandle,
		deletedHandle:     deletedHandle,
		firstReplacement:  firstReplacement,
		secondReplacement: secondReplacement,
		latestRow:         latestRow,
		committedRecords:  committedRecords,
	}
}

func openVacuumTestRelation(t *testing.T, root, name string, pageSize int) (*HeapManager, *Relation) {
	t.Helper()

	return openVacuumTestRelationWithDescriptor(t, root, pagedTestTableDescriptor(name), pageSize)
}

func openVacuumTestRelationWithDescriptor(
	t *testing.T,
	root string,
	desc *catalog.TableDescriptor,
	pageSize int,
) (*HeapManager, *Relation) {
	t.Helper()

	manager, err := OpenHeapManager(root, pageSize, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	if err := manager.CreateTable(nil, desc); err != nil && !errors.Is(err, ErrRelationExists) {
		_ = manager.Close()
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		_ = manager.Close()
		t.Fatalf("open relation: %v", err)
	}
	return manager, relation
}

func readVacuumPageState(t *testing.T, relation *Relation, pageID PageID) vacuumPageState {
	t.Helper()

	page, err := relation.manager.Fetch(pageID)
	if err != nil {
		t.Fatalf("fetch page %d: %v", pageID, err)
	}
	defer func() {
		_ = relation.manager.Unpin(page, false)
	}()

	header, err := page.Header()
	if err != nil {
		t.Fatalf("decode page %d header: %v", pageID, err)
	}

	return vacuumPageState{
		header:    header,
		freeSpace: int(header.Upper) - int(header.Lower),
	}
}

func invokeRelationVacuum(t *testing.T, relation *Relation) {
	t.Helper()

	method := reflect.ValueOf(relation).MethodByName("Vacuum")
	if !method.IsValid() {
		t.Fatalf("Relation.Vacuum method not found")
	}

	methodType := method.Type()
	if methodType.NumIn() != 0 {
		t.Fatalf("Relation.Vacuum signature = %s, want no arguments", methodType)
	}
	if methodType.NumOut() > 1 {
		t.Fatalf("Relation.Vacuum signature = %s, want at most one return value", methodType)
	}

	results := method.Call(nil)
	if methodType.NumOut() == 0 {
		return
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if !methodType.Out(0).Implements(errorType) {
		t.Fatalf("Relation.Vacuum return type = %s, want error", methodType.Out(0))
	}
	if !results[0].IsNil() {
		t.Fatalf("relation vacuum: %v", results[0].Interface())
	}
}

func assertPagedRecordsEqual(t *testing.T, got, want []storage.Record) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(got records) = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Handle != want[index].Handle {
			t.Fatalf("record[%d] handle = %#v, want %#v", index, got[index].Handle, want[index].Handle)
		}
		assertStorageRowEqual(t, got[index].Row, want[index].Row)
	}
}
