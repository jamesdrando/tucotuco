package paged

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/wal"
)

var (
	// ErrInvalidRelation reports a malformed relation descriptor or handle.
	ErrInvalidRelation = errors.New("paged: invalid relation")
	// ErrRelationExists reports an attempt to create an existing relation file.
	ErrRelationExists = errors.New("paged: relation already exists")
	// ErrRelationNotFound reports a missing relation file.
	ErrRelationNotFound = errors.New("paged: relation not found")
	// ErrRowNotFound reports a missing or dead row handle target.
	ErrRowNotFound = errors.New("paged: row not found")
	// ErrRowTooLarge reports a row that cannot fit in any heap page.
	ErrRowTooLarge = errors.New("paged: row too large for heap page")
	// ErrUnsupportedType reports a schema type the tuple codec does not support yet.
	ErrUnsupportedType = errors.New("paged: unsupported column type")
	// ErrTypeMismatch reports a row value that does not match the relation schema.
	ErrTypeMismatch = errors.New("paged: row value does not match column type")
)

const (
	relationMetadataMagic   uint32 = 0x4e4c4552 // "RELN"
	relationMetadataVersion uint16 = 1
	relationMetadataSize           = 40
)

// RelationMetadata is the relation-local metadata persisted on page 0.
type RelationMetadata struct {
	Table         storage.TableID
	PageSize      int
	FirstHeapPage PageID
	LastHeapPage  PageID
	InsertHint    PageID
}

// HeapManager maps logical tables onto per-table relation files under one root.
type HeapManager struct {
	mu        sync.Mutex
	root      string
	wal       *wal.Log
	pageSize  int
	cacheSize int
	relations map[storage.TableID]*Relation
	closed    bool
}

// Relation owns one table heap file and its relation-local buffer pool.
type Relation struct {
	mu      sync.Mutex
	desc    *catalog.TableDescriptor
	path    string
	wal     *wal.Log
	store   PageStore
	manager *Manager
	closed  bool
}

type pinnedHeapPage struct {
	page  *Page
	heap  *heapPage
	dirty bool
}

type resolvedRowLocation struct {
	rootHandle    storage.RowHandle
	currentHandle storage.RowHandle
	rootPage      *pinnedHeapPage
	currentPage   *pinnedHeapPage
	rootSlot      slotEntry
	currentSlot   slotEntry
}

func (l *resolvedRowLocation) livePage() *pinnedHeapPage {
	if l == nil || l.currentPage == nil {
		return l.rootPage
	}
	return l.currentPage
}

func (l *resolvedRowLocation) redirected() bool {
	return l != nil && l.currentPage != nil
}

// OpenHeapManager opens or creates a relation-file root directory.
func OpenHeapManager(root string, pageSize, cacheSize int) (*HeapManager, error) {
	if root == "" || pageSize < pageHeaderSize || cacheSize <= 0 {
		return nil, ErrInvalidConfig
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	walLog, err := wal.Open(walFilePath(root))
	if err != nil {
		return nil, err
	}

	return &HeapManager{
		root:      root,
		wal:       walLog,
		pageSize:  pageSize,
		cacheSize: cacheSize,
		relations: make(map[storage.TableID]*Relation),
	}, nil
}

// CreateTable creates a new empty relation file for desc.
func (m *HeapManager) CreateTable(_ storage.Transaction, desc *catalog.TableDescriptor) error {
	if err := validateRelationDescriptor(desc); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}
	if _, ok := m.relations[desc.ID]; ok {
		return ErrRelationExists
	}

	path := relationFilePath(m.root, desc.ID)
	if _, err := os.Stat(path); err == nil {
		return ErrRelationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	relation, err := openRelation(path, cloneRelationDescriptor(desc), m.wal, m.pageSize, m.cacheSize, true)
	if err != nil {
		return err
	}
	m.relations[desc.ID] = relation
	return nil
}

// DropTable closes and removes the relation file for desc.
func (m *HeapManager) DropTable(_ storage.Transaction, desc *catalog.TableDescriptor) error {
	if err := validateRelationDescriptor(desc); err != nil {
		return err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	relation := m.relations[desc.ID]
	delete(m.relations, desc.ID)
	m.mu.Unlock()

	path := relationFilePath(m.root, desc.ID)
	firstErr := error(nil)
	if relation != nil {
		if err := relation.Close(); err != nil {
			firstErr = err
		}
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) && firstErr == nil {
			return ErrRelationNotFound
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// OpenRelation returns an open relation handle for desc.
func (m *HeapManager) OpenRelation(desc *catalog.TableDescriptor) (*Relation, error) {
	if err := validateRelationDescriptor(desc); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrClosed
	}
	if relation, ok := m.relations[desc.ID]; ok {
		return relation, nil
	}

	path := relationFilePath(m.root, desc.ID)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrRelationNotFound
		}
		return nil, err
	}

	relation, err := openRelation(path, cloneRelationDescriptor(desc), m.wal, m.pageSize, m.cacheSize, false)
	if err != nil {
		return nil, err
	}
	m.relations[desc.ID] = relation
	return relation, nil
}

// Close closes every cached relation handle.
func (m *HeapManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true

	relations := make([]*Relation, 0, len(m.relations))
	for id, relation := range m.relations {
		delete(m.relations, id)
		relations = append(relations, relation)
	}
	m.mu.Unlock()

	var firstErr error
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		if err := relation.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.wal != nil {
		if err := m.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Metadata returns the persisted relation metadata from page 0.
func (r *Relation) Metadata() (RelationMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return RelationMetadata{}, ErrClosed
	}

	page, err := r.manager.Fetch(0)
	if err != nil {
		return RelationMetadata{}, err
	}
	defer func() {
		_ = r.manager.Unpin(page, false)
	}()

	return decodeRelationMetadataPage(page)
}

// Insert appends row to the relation heap and returns its page/slot handle.
func (r *Relation) Insert(row storage.Row) (storage.RowHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return storage.RowHandle{}, ErrClosed
	}

	tuple, err := encodeRowTuple(r.desc, row)
	if err != nil {
		return storage.RowHandle{}, err
	}
	return r.insertEncodedTuple(tuple)
}

// Update replaces the row stored at handle while preserving the original
// handle through one redirect hop when the tuple must move.
func (r *Relation) Update(handle storage.RowHandle, row storage.Row) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrClosed
	}
	if !handle.Valid() || handle.Page == 0 {
		return ErrRowNotFound
	}

	tuple, err := encodeRowTuple(r.desc, row)
	if err != nil {
		return err
	}
	if len(tuple)+pageSlotSize > r.store.PageSize()-pageHeaderSize {
		return ErrRowTooLarge
	}

	location, err := r.resolveRowForWrite(handle)
	if err != nil {
		return err
	}
	defer r.releaseResolvedRow(location)

	livePage := location.livePage()
	if len(tuple) <= int(location.currentSlot.Length) {
		if err := r.mutateAndLogHeapPage(livePage, func(heap *heapPage) error {
			return heap.rewriteTuple(location.currentHandle.Slot, tuple)
		}); err != nil {
			return err
		}
		return nil
	}

	newHandle, err := r.insertEncodedTuple(tuple)
	if err != nil {
		return err
	}

	if err := r.mutateAndLogHeapPage(location.rootPage, func(heap *heapPage) error {
		return heap.installRedirect(location.rootHandle.Slot, newHandle)
	}); err != nil {
		return err
	}

	if location.redirected() {
		if err := r.mutateAndLogHeapPage(livePage, func(heap *heapPage) error {
			return heap.markDead(location.currentHandle.Slot)
		}); err != nil {
			return err
		}
	}

	return nil
}

// Delete marks the addressed row dead. Redirect roots and their current target
// both become non-visible so the original handle fails cleanly.
func (r *Relation) Delete(handle storage.RowHandle) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrClosed
	}
	if !handle.Valid() || handle.Page == 0 {
		return ErrRowNotFound
	}

	location, err := r.resolveRowForWrite(handle)
	if err != nil {
		return err
	}
	defer r.releaseResolvedRow(location)

	if err := r.mutateAndLogHeapPage(location.rootPage, func(heap *heapPage) error {
		return heap.markDead(location.rootHandle.Slot)
	}); err != nil {
		return err
	}

	if location.redirected() {
		livePage := location.livePage()
		if err := r.mutateAndLogHeapPage(livePage, func(heap *heapPage) error {
			return heap.markDead(location.currentHandle.Slot)
		}); err != nil {
			return err
		}
	}

	return nil
}

// Lookup resolves a page/slot handle to its stored row.
func (r *Relation) Lookup(handle storage.RowHandle) (storage.Row, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return storage.Row{}, ErrClosed
	}
	if !handle.Valid() || handle.Page == 0 {
		return storage.Row{}, ErrRowNotFound
	}

	return r.lookupHandle(handle, false)
}

// Close flushes dirty pages and closes the relation file.
func (r *Relation) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	return r.manager.Close()
}

func openRelation(
	path string,
	desc *catalog.TableDescriptor,
	walLog *wal.Log,
	pageSize int,
	cacheSize int,
	create bool,
) (*Relation, error) {
	store, err := OpenFileStore(path, pageSize)
	if err != nil {
		return nil, err
	}

	manager, err := newManager(store, cacheSize, walLog)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	relation := &Relation{
		desc:    desc,
		path:    path,
		wal:     walLog,
		store:   store,
		manager: manager,
	}

	if create {
		if err := relation.initializeMetadata(); err != nil {
			_ = relation.Close()
			return nil, err
		}
		return relation, nil
	}

	metadata, err := relation.Metadata()
	if err != nil {
		_ = relation.Close()
		return nil, err
	}
	if metadata.Table != desc.ID {
		_ = relation.Close()
		return nil, fmt.Errorf("paged: relation metadata mismatch: have %s want %s", metadata.Table, desc.ID)
	}
	if metadata.PageSize != pageSize {
		_ = relation.Close()
		return nil, fmt.Errorf("paged: relation page size mismatch: have %d want %d", metadata.PageSize, pageSize)
	}

	return relation, nil
}

func (r *Relation) initializeMetadata() error {
	page, err := r.manager.Fetch(0)
	if err != nil {
		return err
	}
	if err := r.mutateAndLogPage(page, func() error {
		return applyRelationMetadata(page.data, RelationMetadata{
			Table:         r.desc.ID,
			PageSize:      r.store.PageSize(),
			FirstHeapPage: 0,
			LastHeapPage:  0,
			InsertHint:    0,
		})
	}); err != nil {
		_ = r.manager.Unpin(page, false)
		return err
	}
	if err := r.manager.Unpin(page, true); err != nil {
		return err
	}
	return r.manager.Flush(0)
}

func (r *Relation) selectInsertPage(tuple []byte, metadata RelationMetadata) (*Page, bool, error) {
	pageCount, err := r.store.PageCount()
	if err != nil {
		return nil, false, err
	}

	if metadata.InsertHint > 0 && metadata.InsertHint < pageCount {
		page, err := r.manager.Fetch(metadata.InsertHint)
		if err == nil {
			heap, heapErr := newHeapPage(page)
			if heapErr == nil && heap.canFit(len(tuple)) {
				return page, false, nil
			}
			_ = r.manager.Unpin(page, false)
		}
	}

	for pageID := PageID(1); pageID < pageCount; pageID++ {
		if pageID == metadata.InsertHint {
			continue
		}

		page, err := r.manager.Fetch(pageID)
		if err != nil {
			return nil, false, err
		}
		heap, err := newHeapPage(page)
		if err != nil {
			_ = r.manager.Unpin(page, false)
			return nil, false, err
		}
		if heap.canFit(len(tuple)) {
			return page, false, nil
		}
		if err := r.manager.Unpin(page, false); err != nil {
			return nil, false, err
		}
	}

	page, err := r.manager.NewPage(PageTypeHeap)
	if err != nil {
		return nil, false, err
	}
	return page, true, nil
}

func (r *Relation) insertEncodedTuple(tuple []byte) (storage.RowHandle, error) {
	if len(tuple)+pageSlotSize > r.store.PageSize()-pageHeaderSize {
		return storage.RowHandle{}, ErrRowTooLarge
	}

	metadataPage, err := r.manager.Fetch(0)
	if err != nil {
		return storage.RowHandle{}, err
	}
	metadata, err := decodeRelationMetadataPage(metadataPage)
	if err != nil {
		_ = r.manager.Unpin(metadataPage, false)
		return storage.RowHandle{}, err
	}
	if err := r.manager.Unpin(metadataPage, false); err != nil {
		return storage.RowHandle{}, err
	}

	targetPage, targetDirty, err := r.selectInsertPage(tuple, metadata)
	if err != nil {
		return storage.RowHandle{}, err
	}
	defer func() {
		if targetPage != nil {
			_ = r.manager.Unpin(targetPage, targetDirty)
		}
	}()

	heap, err := newHeapPage(targetPage)
	if err != nil {
		return storage.RowHandle{}, err
	}
	slotIndex := uint16(0)
	if err := r.mutateAndLogPage(targetPage, func() error {
		insertedSlot, err := heap.insertTuple(tuple)
		if err != nil {
			return err
		}
		slotIndex = insertedSlot
		return heap.syncHeader()
	}); err != nil {
		return storage.RowHandle{}, err
	}
	targetDirty = true

	pageID := targetPage.ID()
	r.noteInsertedPage(&metadata, heap, pageID)
	if err := r.manager.Unpin(targetPage, targetDirty); err != nil {
		return storage.RowHandle{}, err
	}
	targetPage = nil

	metadataPage, err = r.manager.Fetch(0)
	if err != nil {
		return storage.RowHandle{}, err
	}
	if err := r.mutateAndLogPage(metadataPage, func() error {
		return applyRelationMetadata(metadataPage.data, metadata)
	}); err != nil {
		_ = r.manager.Unpin(metadataPage, false)
		return storage.RowHandle{}, err
	}
	if err := r.manager.Unpin(metadataPage, true); err != nil {
		return storage.RowHandle{}, err
	}

	return storage.RowHandle{Page: uint64(pageID), Slot: uint64(slotIndex)}, nil
}

func (r *Relation) noteInsertedPage(metadata *RelationMetadata, heap *heapPage, pageID PageID) {
	if metadata == nil {
		return
	}
	if metadata.FirstHeapPage == 0 {
		metadata.FirstHeapPage = pageID
	}
	if pageID > metadata.LastHeapPage {
		metadata.LastHeapPage = pageID
	}
	if heap != nil && heap.canFit(tupleHeaderSize) {
		metadata.InsertHint = pageID
	} else {
		metadata.InsertHint = 0
	}
}

func (r *Relation) lookupHandle(handle storage.RowHandle, redirected bool) (storage.Row, error) {
	page, err := r.manager.Fetch(PageID(handle.Page))
	if err != nil {
		if errors.Is(err, ErrPageNotFound) {
			return storage.Row{}, ErrRowNotFound
		}
		return storage.Row{}, err
	}
	defer func() {
		_ = r.manager.Unpin(page, false)
	}()

	heap, err := newHeapPage(page)
	if err != nil {
		return storage.Row{}, err
	}

	tuple, slot, err := heap.tuple(handle.Slot)
	if err != nil {
		return storage.Row{}, err
	}
	if slot.Flags == slotFlagRedirect {
		if redirected {
			return storage.Row{}, errors.New("paged: redirect chain exceeds one hop")
		}
		target, err := heap.redirectHandle(slot)
		if err != nil {
			return storage.Row{}, err
		}
		if target.Page == 0 {
			return storage.Row{}, ErrRowNotFound
		}
		return r.lookupHandle(target, true)
	}

	return decodeRowTuple(r.desc, tuple)
}

func (r *Relation) resolveRowForWrite(handle storage.RowHandle) (*resolvedRowLocation, error) {
	rootPage, rootSlot, err := r.fetchRowLocation(handle)
	if err != nil {
		return nil, err
	}

	location := &resolvedRowLocation{
		rootHandle:    handle,
		currentHandle: handle,
		rootPage:      rootPage,
		rootSlot:      rootSlot,
		currentSlot:   rootSlot,
	}

	if rootSlot.Flags != slotFlagRedirect {
		return location, nil
	}

	target, err := rootPage.heap.redirectHandle(rootSlot)
	if err != nil {
		r.releasePinnedHeapPage(rootPage)
		return nil, err
	}
	if !target.Valid() || target.Page == 0 || target == handle {
		r.releasePinnedHeapPage(rootPage)
		return nil, ErrRowNotFound
	}

	currentPage, currentSlot, err := r.fetchRowLocation(target)
	if err != nil {
		r.releasePinnedHeapPage(rootPage)
		return nil, err
	}
	if currentSlot.Flags == slotFlagRedirect {
		r.releasePinnedHeapPage(currentPage)
		r.releasePinnedHeapPage(rootPage)
		return nil, errors.New("paged: redirect chain exceeds one hop")
	}

	location.currentHandle = target
	location.currentPage = currentPage
	location.currentSlot = currentSlot
	return location, nil
}

func (r *Relation) fetchRowLocation(handle storage.RowHandle) (*pinnedHeapPage, slotEntry, error) {
	page, err := r.fetchHeapPage(PageID(handle.Page))
	if err != nil {
		return nil, slotEntry{}, err
	}

	slot, err := page.heap.readSlot(handle.Slot)
	if err != nil {
		r.releasePinnedHeapPage(page)
		return nil, slotEntry{}, err
	}

	switch slot.Flags {
	case slotFlagLive, slotFlagRedirect:
		return page, slot, nil
	case slotFlagDead, slotFlagUnused:
		r.releasePinnedHeapPage(page)
		return nil, slotEntry{}, ErrRowNotFound
	default:
		r.releasePinnedHeapPage(page)
		return nil, slotEntry{}, fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", handle.Page, handle.Slot, slot.Flags)
	}
}

func (r *Relation) fetchHeapPage(pageID PageID) (*pinnedHeapPage, error) {
	page, err := r.manager.Fetch(pageID)
	if err != nil {
		if errors.Is(err, ErrPageNotFound) {
			return nil, ErrRowNotFound
		}
		return nil, err
	}

	heap, err := newHeapPage(page)
	if err != nil {
		_ = r.manager.Unpin(page, false)
		return nil, err
	}

	return &pinnedHeapPage{page: page, heap: heap}, nil
}

func (r *Relation) releaseResolvedRow(location *resolvedRowLocation) {
	if location == nil {
		return
	}
	r.releasePinnedHeapPage(location.currentPage)
	r.releasePinnedHeapPage(location.rootPage)
}

func (r *Relation) releasePinnedHeapPage(page *pinnedHeapPage) {
	if page == nil || page.page == nil {
		return
	}
	_ = r.manager.Unpin(page.page, page.dirty)
}

func validateRelationDescriptor(desc *catalog.TableDescriptor) error {
	if desc == nil || !desc.ID.Valid() || len(desc.Columns) == 0 {
		return ErrInvalidRelation
	}
	return nil
}

func cloneRelationDescriptor(desc *catalog.TableDescriptor) *catalog.TableDescriptor {
	if desc == nil {
		return nil
	}

	clone := &catalog.TableDescriptor{
		ID:      desc.ID,
		Columns: make([]catalog.ColumnDescriptor, len(desc.Columns)),
	}
	copy(clone.Columns, desc.Columns)
	return clone
}

func relationFilePath(root string, id storage.TableID) string {
	return filepath.Join(root, relationFileName(id))
}

func relationFileName(id storage.TableID) string {
	return fmt.Sprintf("%x_%x.heap", []byte(id.Schema), []byte(id.Name))
}

func applyRelationMetadata(page []byte, metadata RelationMetadata) error {
	header, err := DecodePageHeader(page)
	if err != nil {
		return err
	}
	if header.PageType != PageTypeMetadata || header.PageID != 0 {
		return fmt.Errorf("paged: page 0 is not a metadata page")
	}

	payload, err := encodeRelationMetadata(metadata)
	if err != nil {
		return err
	}
	if len(payload) > int(header.Special)-pageHeaderSize {
		return fmt.Errorf("paged: relation metadata payload %d exceeds page capacity", len(payload))
	}

	for index := pageHeaderSize; index < len(page); index++ {
		page[index] = 0
	}

	header.Lower = pageHeaderSize
	header.SlotCount = 0
	header.Upper = header.Special - uint16(len(payload))
	copy(page[header.Upper:header.Special], payload)
	return EncodePageHeader(page, header)
}

func decodeRelationMetadataPage(page *Page) (RelationMetadata, error) {
	if page == nil {
		return RelationMetadata{}, ErrInvalidRelation
	}

	header, err := page.Header()
	if err != nil {
		return RelationMetadata{}, err
	}
	if header.PageType != PageTypeMetadata || header.PageID != 0 {
		return RelationMetadata{}, fmt.Errorf("paged: page %d is not relation metadata", page.ID())
	}
	if header.Upper < pageHeaderSize || header.Upper > header.Special {
		return RelationMetadata{}, fmt.Errorf("paged: metadata page has invalid payload bounds")
	}

	return decodeRelationMetadata(page.data[header.Upper:header.Special])
}

func encodeRelationMetadata(metadata RelationMetadata) ([]byte, error) {
	if !metadata.Table.Valid() || metadata.PageSize < pageHeaderSize {
		return nil, ErrInvalidRelation
	}

	schemaBytes := []byte(metadata.Table.Schema)
	nameBytes := []byte(metadata.Table.Name)
	payload := make([]byte, relationMetadataSize+len(schemaBytes)+len(nameBytes))
	binary.LittleEndian.PutUint32(payload[0x00:0x04], relationMetadataMagic)
	binary.LittleEndian.PutUint16(payload[0x04:0x06], relationMetadataVersion)
	binary.LittleEndian.PutUint16(payload[0x06:0x08], uint16(len(schemaBytes)))
	binary.LittleEndian.PutUint16(payload[0x08:0x0a], uint16(len(nameBytes)))
	binary.LittleEndian.PutUint32(payload[0x0c:0x10], uint32(metadata.PageSize))
	binary.LittleEndian.PutUint64(payload[0x10:0x18], uint64(metadata.FirstHeapPage))
	binary.LittleEndian.PutUint64(payload[0x18:0x20], uint64(metadata.LastHeapPage))
	binary.LittleEndian.PutUint64(payload[0x20:0x28], uint64(metadata.InsertHint))
	copy(payload[relationMetadataSize:], schemaBytes)
	copy(payload[relationMetadataSize+len(schemaBytes):], nameBytes)
	return payload, nil
}

func decodeRelationMetadata(payload []byte) (RelationMetadata, error) {
	if len(payload) < relationMetadataSize {
		return RelationMetadata{}, fmt.Errorf("paged: relation metadata payload too small: %d", len(payload))
	}
	if binary.LittleEndian.Uint32(payload[0x00:0x04]) != relationMetadataMagic {
		return RelationMetadata{}, errors.New("paged: invalid relation metadata magic")
	}
	if binary.LittleEndian.Uint16(payload[0x04:0x06]) != relationMetadataVersion {
		return RelationMetadata{}, fmt.Errorf(
			"paged: unsupported relation metadata version %d",
			binary.LittleEndian.Uint16(payload[0x04:0x06]),
		)
	}

	schemaLen := int(binary.LittleEndian.Uint16(payload[0x06:0x08]))
	nameLen := int(binary.LittleEndian.Uint16(payload[0x08:0x0a]))
	if relationMetadataSize+schemaLen+nameLen > len(payload) {
		return RelationMetadata{}, fmt.Errorf("paged: relation metadata names exceed payload")
	}

	start := relationMetadataSize
	schema := string(payload[start : start+schemaLen])
	start += schemaLen
	name := string(payload[start : start+nameLen])

	return RelationMetadata{
		Table: storage.TableID{
			Schema: schema,
			Name:   name,
		},
		PageSize:      int(binary.LittleEndian.Uint32(payload[0x0c:0x10])),
		FirstHeapPage: PageID(binary.LittleEndian.Uint64(payload[0x10:0x18])),
		LastHeapPage:  PageID(binary.LittleEndian.Uint64(payload[0x18:0x20])),
		InsertHint:    PageID(binary.LittleEndian.Uint64(payload[0x20:0x28])),
	}, nil
}
