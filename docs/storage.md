# Storage Layout

This document defines the Phase 2 paged-storage format used by the heap
storage work in `T-121` through `T-125`.

The design is intentionally small and explicit:

- slotted pages
- fixed page header
- slot directory at the front of the page
- tuple payloads packed from the back of the page
- opaque row handles for stable row identity
- tombstones and redirection for update/delete compatibility
- reserved header space for WAL, buffer-pool, and MVCC work

The storage layer remains an internal implementation detail. Query execution
continues to work with logical rows (`[]Value`), not physical page images.

## 1. Format goals

| Goal | Design consequence |
|------|--------------------|
| Simple to implement | One page format for heap data, one fixed tuple layout, one slot directory model |
| Stable row identity | Row handles point to a slot, not a byte offset |
| Update-friendly | Updates may redirect instead of rewriting in place |
| Delete-friendly | Deletes leave tombstones until space is reclaimed |
| WAL-ready | Every page carries a page LSN and checksum field |
| MVCC-ready | Tuple header reserves room for versioning metadata |
| Buffer-pool-ready | Header contains enough metadata to validate and flush pages safely |

## 2. Scope

This document covers:

- heap pages that store table rows
- relation-file page numbering
- tuple and slot directory layout
- free-space tracking inside a page
- stable row-handle mapping
- physical update and delete behavior

This document does not define:

- B-tree or other index page layouts
- WAL record format
- MVCC visibility rules
- transaction IDs and commit state storage

Those pieces are expected to build on the invariants defined here.

## 3. File and page model

### 3.1 Relation file

Each table heap is stored in a relation file. Page numbering is file-relative.

| Page number | Meaning |
|-------------|---------|
| `0` | Relation metadata page |
| `1..N` | Heap data pages |

The relation metadata page is reserved for future file-level bookkeeping such
as schema ID, page size, and growth hints. Heap rows never live on page `0`.

### 3.2 Page size

| Field | Value |
|-------|-------|
| Default page size | `8192` bytes |
| Required alignment | 8-byte alignment for the page header and tuple payloads where practical |
| Byte order | Little-endian on disk |

The file format must store the page size in metadata so the engine can reject
or migrate incompatible files later. Phase 2 may start with a single supported
page size, but the header must remain versioned.

## 4. Page layout

A heap page is a fixed-size slotted page. The front of the page grows upward
for metadata and slot entries. The back of the page grows downward for tuple
payloads.

### 4.1 Layout overview

```text
+----------------------+  low addresses
| Page header          |
| Slot directory       |
| Free space           |
| Tuple payloads       |
+----------------------+  high addresses
```

### 4.2 Page header

The heap page header is fixed at 64 bytes. Fields are stored in little-endian
format.

| Offset | Size | Field | Meaning |
|--------|------|-------|---------|
| 0x00 | 4 | `magic` | Page signature for heap pages |
| 0x04 | 2 | `format_version` | On-disk layout version |
| 0x06 | 1 | `page_type` | Heap, metadata, or reserved |
| 0x07 | 1 | `flags` | Page state flags |
| 0x08 | 8 | `page_id` | Relation-relative page number |
| 0x10 | 8 | `page_lsn` | Last WAL LSN applied to this page |
| 0x18 | 4 | `checksum` | Page checksum, computed with this field zeroed |
| 0x1C | 2 | `lower` | First byte after slot directory |
| 0x1E | 2 | `upper` | First byte of free space from the back of the page |
| 0x20 | 2 | `special` | Start of special space, if any |
| 0x22 | 2 | `slot_count` | Number of slot entries in the directory |
| 0x24 | 2 | `dead_bytes` | Approximate bytes owned by dead tuples or redirections |
| 0x26 | 2 | `reserved0` | Reserved |
| 0x28 | 8 | `reserved1` | Reserved for future WAL/MVCC/buffer-pool needs |
| 0x30 | 16 | `reserved2` | Reserved |

### 4.3 Header flags

| Flag | Meaning |
|------|---------|
| `page_flag_dirty_hint` | Page has been modified since last clean checkpoint or flush hint |
| `page_flag_has_redirects` | At least one slot is a forwarding pointer |
| `page_flag_has_dead_tuples` | At least one slot is dead but not compacted |
| `page_flag_all_visible` | Reserved for later MVCC visibility map integration |
| `page_flag_reserved` | Must be zero in Phase 2 |

The `dirty` state itself is not persisted as a correctness mechanism. It is a
buffer-pool concern. The flag bits exist only as hints and future hooks.

### 4.4 Slot directory

The slot directory follows immediately after the header. Each slot entry is 8
bytes.

| Offset within slot | Size | Field | Meaning |
|--------------------|------|-------|---------|
| 0x00 | 2 | `offset` | Byte offset of payload within the page |
| 0x02 | 2 | `length` | Payload length in bytes |
| 0x04 | 2 | `flags` | Slot state flags |
| 0x06 | 2 | `generation` | Monotonic reuse counter for stale handle protection |

Slot numbering is zero-based. Slot `0` is the first entry in the directory.

### 4.5 Slot states

| Slot flag | Meaning |
|-----------|---------|
| `slot_live` | Slot points to an in-page tuple payload |
| `slot_dead` | Slot is a tombstone after delete |
| `slot_redirect` | Slot holds a forwarding record to another row handle |
| `slot_unused` | Slot entry exists but has never been assigned |

Exactly one of `slot_live`, `slot_dead`, `slot_redirect`, or `slot_unused`
must be set.

### 4.6 Free space

Free space is the interval `[lower, upper)`.

| Pointer | Meaning |
|---------|---------|
| `lower` | First unused byte after the slot directory |
| `upper` | First byte of tuple payload space from the back of the page |

Invariants:

1. `header_len <= lower <= upper <= special <= page_size`
2. The slot directory occupies `[header_len, lower)`
3. Payload bytes occupy `[upper, special)`
4. The free-space interval is contiguous unless there are dead tuples or redirects that can be compacted away

`dead_bytes` is an advisory counter used to decide when compaction is worth the
cost. It does not need to be exact, but it must be monotonic enough to avoid
starving reclamation.

## 5. Tuple format

Tuples are row-oriented records. They are stored in catalog column order, not
columnar form.

### 5.1 Tuple header

Each tuple begins with a fixed header.

| Offset | Size | Field | Meaning |
|--------|------|-------|---------|
| 0x00 | 2 | `tuple_version` | Tuple layout version |
| 0x02 | 2 | `tuple_flags` | Live, deleted, redirected, reserved |
| 0x04 | 4 | `payload_len` | Bytes following the fixed header |
| 0x08 | 4 | `nullmap_len` | Bytes used by the NULL bitmap |
| 0x0C | 8 | `xmin` | Reserved transaction ID / version origin for future MVCC |
| 0x14 | 8 | `xmax` | Reserved transaction ID / version end for future MVCC |
| 0x1C | 8 | `forward_ptr` | Reserved forwarding pointer for future version chaining / HOT-style movement |
| 0x24 | 8 | `reserved` | Reserved for additional tuple-version metadata |

The tuple header is intentionally versioned so the format can evolve. Version
`1` reserves explicit space for MVCC/version metadata even before those fields
are semantically active. That allows `T-125` to enable tuple visibility rules
without changing the page layout.

### 5.2 Tuple body

The tuple body has the following logical structure:

1. NULL bitmap
2. column payloads in table column order

The NULL bitmap has one bit per nullable column, in column order. A set bit
means the column is NULL.

Type encoding rules:

Column order, nullability, declared type, and codec selection come from the
catalog schema for the owning relation. The tuple itself does not carry per-row
type descriptors.

| Type class | Storage rule |
|------------|--------------|
| Fixed-width numerics and booleans | Stored in native fixed-width binary form |
| Dates and timestamps | Stored in canonical binary form defined by the type codec layer |
| Variable-length strings and binaries | Stored as length-prefixed byte sequences |
| TEXT / CLOB / BLOB | Stored as length-prefixed byte sequences, possibly with future out-of-line support |
| DECIMAL / NUMERIC | Stored in a canonical binary codec, not as text |
| ROW / ARRAY | Stored recursively using the same canonical codecs |

For fixed-width `CHAR(n)`, the stored value is the padded canonical form.
For `VARCHAR(n)` and other varlen values, the encoded payload length is the
actual byte count, not the declared maximum.

### 5.3 Tuple payload invariants

1. Tuple payloads are immutable once written.
2. A live slot points at exactly one tuple payload.
3. A tuple payload never overlaps another tuple payload.
4. A tuple payload never extends outside `[upper, special)`.
5. Tuple bytes are not interpreted by the page manager beyond length and state.

## 6. Row handles

Phase 2 uses the existing caller-visible `storage.RowHandle` shape: `Page +
Slot` only. The handle is intentionally small and stable so the storage API
remains aligned with the current interface.

### 6.1 Logical shape

The handle must identify:

| Component | Purpose |
|-----------|---------|
| Page number | Which page inside the file contains the slot |
| Slot number | Which slot entry references the tuple |

The handle is opaque to callers. The on-disk and in-memory packing are
implementation details, but the representation must remain a compact scalar
value so the executor and storage manager can pass it cheaply. Relation identity
is resolved by the surrounding table/heap context, not embedded into the row
handle.

### 6.2 Handle rules

1. A handle is valid only for a single logical row version at a time.
2. A stale handle must fail cleanly rather than silently pointing at a new row.
3. Redirection must preserve row identity until the old version is no longer reachable.
4. Slot reuse protection is internal: the page may keep a generation counter in
   slot metadata, but that counter is not part of the current caller-visible
   handle.

### 6.3 Handle resolution

Handle lookup follows this order:

1. Resolve relation ID to the heap file.
2. Load the target page.
3. If the slot is live, return the tuple.
4. If the slot is redirected, follow the forwarding pointer once.
5. If the slot is dead or unused, report row-not-found.

Redirect chains must not become unbounded. A page may store one forwarding hop
per logical row. Longer chains are a bug.

## 7. Insert, update, and delete behavior

### 7.1 Insert

Insert chooses a page with enough contiguous free space for:

- the tuple payload
- a new slot entry if no reusable slot is available

Insertion order inside a page:

1. Reuse an unused slot if one exists.
2. Otherwise allocate a new slot at the end of the slot directory.
3. Reserve payload space by moving `upper` downward.
4. Write the tuple bytes.
5. Publish the slot entry.

The slot entry must not point to an uninitialized payload.

### 7.2 Update

Updates follow three cases:

| Case | Behavior |
|------|----------|
| New tuple fits in the same payload space | Rewrite in place only if the handle does not need to move |
| New tuple does not fit, but a nearby slot can be reused | Move the row, leave a redirect slot behind |
| Row must move to a different page | Insert the new version elsewhere, leave a redirect slot behind |

The preferred behavior is to preserve handle stability by leaving a redirect
slot when the row moves. In-place rewrite is only acceptable when the physical
location remains valid and no visibility rule is violated.

### 7.3 Delete

Delete is logical first, physical second.

| Step | Effect |
|------|--------|
| Mark slot dead | The row is no longer visible to normal reads |
| Preserve generation | Stale handles cannot be reused accidentally |
| Reclaim later | Space is recovered by compaction or vacuum |

If future MVCC is active, delete should become a versioning operation rather
than an immediate physical removal.

### 7.4 Compaction

Page compaction repacks live tuples toward the end of the page and rewrites
their slot offsets. Compaction is allowed when:

1. fragmentation prevents a needed insert or update
2. dead bytes exceed a threshold
3. the buffer-pool or vacuum manager explicitly requests it

Compaction must preserve slot numbers and slot generations.

## 8. Free-space management

There are two levels of free-space management.

### 8.1 In-page management

The page header is authoritative for in-page space.

| Signal | Use |
|--------|-----|
| `lower` / `upper` | Compute contiguous free space |
| `dead_bytes` | Decide whether compaction is worthwhile |
| slot state flags | Detect reusable or reclaimable entries |

### 8.2 File-level page selection

The heap file manager may maintain an advisory page-selection structure
outside the page format, but correctness must not depend on it.

Rules:

1. A page header must be enough to determine whether the page can accept a row.
2. A stale free-space hint may cause extra page scans, but never corruption.
3. Rebuilt metadata from page headers must be sufficient to recover after restart.

## 9. WAL, buffer-pool, and MVCC compatibility

### 9.1 WAL compatibility

The page header reserves a `page_lsn` field because redo recovery needs a
monotonic page history marker.

Rules:

1. A page cannot be flushed with a checksum or LSN older than the persisted state it represents.
2. Redo should be idempotent when replayed onto a page with the same or newer LSN.
3. The page checksum must be computed after all page bytes are finalized.

### 9.2 Buffer-pool compatibility

The page format must support the usual buffer-pool contract:

1. Pages are pinned while in use.
2. Dirty pages are eventually written back.
3. The on-disk image is self-describing enough to validate corruption.
4. The page header can be read without decoding tuple payloads.

### 9.3 MVCC compatibility

Phase 2 does not require full MVCC yet, but the page format must not block it.

Design hooks:

1. Tuple header has reserved bytes for `xmin` / `xmax` or a compatible versioning record.
2. Redirect slots can represent HOT-style forwarding or version chaining later.
3. Delete does not assume immediate physical reuse.
4. Page compaction must preserve logical version chains.

## 10. Invariants

| Invariant | Why it matters |
|-----------|----------------|
| `magic` and `format_version` identify the page image | Detects wrong file types and incompatible upgrades |
| `lower <= upper` | Prevents slot/payload overlap |
| `slot_count` matches the directory size | Makes handle lookup deterministic |
| Slot offsets point inside the current page | Prevents dangling payload reads |
| Live tuples have positive length | Distinguishes real rows from empty entries |
| Redirect slots point to valid row handles | Keeps row identity stable after movement |
| Dead slots are not returned by scans | Preserves delete semantics |
| Checksum covers the whole page image | Detects torn writes and corruption |
| `page_lsn` is monotonic | Supports redo recovery |

## 11. Worked example

Assume an 8192-byte heap page with a 64-byte header.

| Item | Value |
|------|-------|
| Header | bytes `0..63` |
| Slot directory | bytes `64..87` for 3 slots |
| Free space | bytes `88..7039` |
| Tuple 2 | bytes `7040..7199` |
| Tuple 1 | bytes `7200..7311` |
| Tuple 0 | bytes `7312..7399` |

State after three inserts:

| Field | Value |
|-------|-------|
| `slot_count` | `3` |
| `lower` | `88` |
| `upper` | `7040` |
| `dead_bytes` | `0` |

If tuple 1 is deleted:

| Field | Value |
|-------|-------|
| Slot 1 state | `slot_dead` |
| `dead_bytes` | increases |
| `lower` | unchanged |
| `upper` | unchanged |

If tuple 2 is updated and moved:

| Field | Value |
|-------|-------|
| Old slot | becomes `slot_redirect` or `slot_dead` depending on visibility mode |
| New tuple | written to the new free space location |
| Slot generation | increments when a slot is reused |

## 12. Implementation guidance for T-121 through T-125

1. Build the buffer pool around the page header invariants, not around tuple
   payload internals.
2. Implement heap-page insert/update/delete using slot states and `lower` / `upper`.
3. Treat row handles as opaque and generation-protected from the first storage
   implementation.
4. Add WAL and checksum plumbing before MVCC visibility rules depend on it.
5. Preserve redirect support even if the first implementation mostly rewrites in place.
6. Keep tuple encoding versioned so `T-125` can add version metadata without a
   page-format rewrite.
