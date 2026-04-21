package paged

import (
	"errors"
	"strings"
	"testing"
)

type relationVersionRecoveryFixture struct {
	pageIDs         []PageID
	assertWALImages func(t *testing.T, images map[PageID][]byte)
	assertReopened  func(t *testing.T, relation *Relation)
}

func TestRelationVersionMetadataSurvivesWALAndRecovery(t *testing.T) {
	testCases := []struct {
		name    string
		prepare func(t *testing.T, relation *Relation) relationVersionRecoveryFixture
	}{
		{
			name:    "replacement_version_remains_live",
			prepare: prepareLiveReplacementRecoveryFixture,
		},
		{
			name:    "deleted_replacement_stays_terminal",
			prepare: prepareDeletedReplacementRecoveryFixture,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			desc := pagedTestTableDescriptor(tc.name)
			manager, relation := createRecoveryTestRelation(t, root, desc)
			fixture := tc.prepare(t, relation)
			if err := manager.Close(); err != nil {
				t.Fatalf("close source manager: %v", err)
			}

			latestImages := selectRelationPageImages(t, latestRelationPageImages(t, root, desc), fixture.pageIDs...)
			fixture.assertWALImages(t, latestImages)

			overwriteRelationPageImages(t, root, desc, recoveryTestPageSize, zeroedRelationPageImages(recoveryTestPageSize, fixture.pageIDs...))

			reopenedManager, reopenedRelation := openRecoveryTestRelation(t, root, desc)
			fixture.assertReopened(t, reopenedRelation)
			if err := reopenedManager.Close(); err != nil {
				t.Fatalf("close reopened manager: %v", err)
			}

			assertRelationPageImagesEqual(t, root, desc, recoveryTestPageSize, fixture.pageIDs, latestImages)
		})
	}
}

func prepareLiveReplacementRecoveryFixture(t *testing.T, relation *Relation) relationVersionRecoveryFixture {
	t.Helper()

	handle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert seed row: %v", err)
	}
	want := pagedTestRow(1, strings.Repeat("v", 120))
	if err := relation.Update(handle, want); err != nil {
		t.Fatalf("update row: %v", err)
	}

	rootPageID := PageID(handle.Page)
	replacement := assertHeapPageRedirect(t, relation, rootPageID, handle.Slot)
	replacementPageID := PageID(replacement.Page)
	pageIDs := uniquePageIDs(rootPageID, replacementPageID)

	return relationVersionRecoveryFixture{
		pageIDs: pageIDs,
		assertWALImages: func(t *testing.T, images map[PageID][]byte) {
			t.Helper()

			rootSlot, _ := readPageImageSlotPayload(t, images[rootPageID], rootPageID, handle.Slot)
			if rootSlot.Flags != slotFlagRedirect {
				t.Fatalf("wal root slot flags = 0x%x, want redirect", rootSlot.Flags)
			}

			replacementSnapshot := readPageImageTupleSnapshot(t, images[replacementPageID], replacementPageID, replacement.Slot)
			assertTupleVersionState(t, "wal replacement version", replacementSnapshot, tupleVersionExpectation{
				allowedSlotFlags: []uint16{slotFlagLive},
				xmin:             expectPresent,
				xmax:             expectAbsent,
				forwardPtr:       expectAbsent,
				deletedFlag:      expectAbsent,
			})
		},
		assertReopened: func(t *testing.T, reopened *Relation) {
			t.Helper()

			got, err := reopened.Lookup(handle)
			if err != nil {
				t.Fatalf("lookup updated row after recovery: %v", err)
			}
			assertStorageRowEqual(t, got, want)

			replacementSnapshot := readHeapPageTupleSnapshot(t, reopened, replacementPageID, replacement.Slot)
			assertTupleVersionState(t, "recovered replacement version", replacementSnapshot, tupleVersionExpectation{
				allowedSlotFlags: []uint16{slotFlagLive},
				xmin:             expectPresent,
				xmax:             expectAbsent,
				forwardPtr:       expectAbsent,
				deletedFlag:      expectAbsent,
			})
		},
	}
}

func prepareDeletedReplacementRecoveryFixture(t *testing.T, relation *Relation) relationVersionRecoveryFixture {
	t.Helper()

	handle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert seed row: %v", err)
	}
	if err := relation.Update(handle, pagedTestRow(1, strings.Repeat("w", 120))); err != nil {
		t.Fatalf("update row: %v", err)
	}

	rootPageID := PageID(handle.Page)
	replacement := assertHeapPageRedirect(t, relation, rootPageID, handle.Slot)
	replacementPageID := PageID(replacement.Page)
	liveReplacement := readHeapPageTupleSnapshot(t, relation, replacementPageID, replacement.Slot)
	if err := relation.Delete(handle); err != nil {
		t.Fatalf("delete row: %v", err)
	}

	pageIDs := uniquePageIDs(rootPageID, replacementPageID)
	return relationVersionRecoveryFixture{
		pageIDs: pageIDs,
		assertWALImages: func(t *testing.T, images map[PageID][]byte) {
			t.Helper()

			rootSlot, _ := readPageImageSlotPayload(t, images[rootPageID], rootPageID, handle.Slot)
			if rootSlot.Flags != slotFlagRedirect && rootSlot.Flags != slotFlagDead {
				t.Fatalf("wal deleted root slot flags = 0x%x, want redirect or dead", rootSlot.Flags)
			}

			replacementSnapshot := readPageImageTupleSnapshot(t, images[replacementPageID], replacementPageID, replacement.Slot)
			assertTupleVersionState(t, "wal deleted replacement version", replacementSnapshot, tupleVersionExpectation{
				allowedSlotFlags: []uint16{slotFlagLive, slotFlagDead},
				xmin:             expectPresent,
				xmax:             expectPresent,
				forwardPtr:       expectAbsent,
				deletedFlag:      expectPresent,
			})
			if replacementSnapshot.header.Xmin != liveReplacement.header.Xmin {
				t.Fatalf("wal deleted replacement xmin = %d, want live replacement xmin %d", replacementSnapshot.header.Xmin, liveReplacement.header.Xmin)
			}
		},
		assertReopened: func(t *testing.T, reopened *Relation) {
			t.Helper()

			if _, err := reopened.Lookup(handle); !errors.Is(err, ErrRowNotFound) {
				t.Fatalf("lookup deleted row after recovery error = %v, want %v", err, ErrRowNotFound)
			}

			replacementSnapshot := readHeapPageTupleSnapshot(t, reopened, replacementPageID, replacement.Slot)
			assertTupleVersionState(t, "recovered deleted replacement version", replacementSnapshot, tupleVersionExpectation{
				allowedSlotFlags: []uint16{slotFlagLive, slotFlagDead},
				xmin:             expectPresent,
				xmax:             expectPresent,
				forwardPtr:       expectAbsent,
				deletedFlag:      expectPresent,
			})
			if replacementSnapshot.header.Xmin != liveReplacement.header.Xmin {
				t.Fatalf("recovered deleted replacement xmin = %d, want live replacement xmin %d", replacementSnapshot.header.Xmin, liveReplacement.header.Xmin)
			}
		},
	}
}

func uniquePageIDs(ids ...PageID) []PageID {
	seen := make(map[PageID]struct{}, len(ids))
	unique := make([]PageID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}
