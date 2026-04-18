# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** 2026-04-18
**Phase:** Phase 1 — Core Engine (In-Memory, SQL-92 Subset)
**Milestone in progress:** M1

---

## JUST DONE

Completed `TASKS.md` T-011, T-021, T-031, T-032, and T-071 using `SPEC.md` §3, §4, §5, and §9.

- Added `internal/types.TypeKind` and `internal/types.TypeDesc` with validation, canonical text serialisation/parsing, and round-trip tests built on the native `internal/types.Decimal` change recorded in `SPEC_CHANGE [T-010]`.
- Implemented the Phase 1 lexer core across `internal/token` and `internal/lexer`: token kinds, token metadata, streaming SQL tokenisation, keyword/identifier/string/numeric/operator/punctuation/comment handling, position tracking, a broad lexer test matrix, and `FuzzLexer`.
- Extended `internal/ast` with literal nodes (`integer`, `float`, `string`, `bool`, `NULL`, `param`) plus identifier, qualified-name, and star nodes, along with visitor-dispatch and span coverage.
- Implemented the in-memory row store in `internal/storage/memory` with lazy table materialisation, transaction-scoped pending changes, concurrent-read/exclusive-write access via `sync.RWMutex`, constraint-aware scans, and storage regression coverage.
- Verified `go test ./...` and `go test -race ./internal/types ./internal/token ./internal/lexer ./internal/ast ./internal/storage/...` in the sandbox with writable cache overrides.

---

## NEXT

**Task(s) to execute:** T-012, T-022, T-033, T-040, T-050, T-072, T-073, T-112 (all are unblocked; `internal/parser/` and `internal/catalog/` remain serial)

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Read `SPEC.md §3` plus `SPEC_CHANGE [T-010]` for **T-012** and build on `internal/types/typedesc.go`, `internal/types/value.go`, and `internal/types/decimal.go`.
3. Read `SPEC.md §4` for **T-022** and **T-040**, building directly on `internal/token/token.go`, `internal/token/keywords.go`, and `internal/lexer/lexer.go`.
4. Read `SPEC.md §5` for **T-033** and **T-040**, using the current `internal/ast` visitor and span scaffolding as the extension point.
5. Preserve quoted-vs-unquoted identifier semantics when parser work starts, because `internal/ast.Identifier` currently stores only the lowered identifier text.
6. Read `SPEC.md §6` for **T-050** before touching `internal/catalog/`; keep that package serial.
7. Read `SPEC.md §9` plus `internal/storage/storage.go` and `internal/storage/memory/store.go` for **T-072** and **T-073`.
8. `T-112` is also unblocked and can proceed independently in CI/configuration work.
9. Update `TASKS.md`, `NOW.md`, and `SPEC.md` if later design decisions diverge from the current spec text.

**Spec references:** `SPEC.md` §3, §4, §5, §6, §9, `SPEC_CHANGE [T-010]`

**Estimated parallelism available:** 8 tasks across 7 independent streams (`T-012`, `T-022`, `T-033`, `T-040`, `T-050`, `T-072`/`T-073`, `T-112`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-012 to T-113 (`T-010`, `T-011`, `T-020`, `T-021`, `T-030`, `T-031`, `T-032`, `T-070`, `T-071` complete) |
| M2 — SQL-92 Full + Storage | 🔲 Not started | T-120 to T-171 |
| M3 — SQL:1999 | 🔲 Not started | T-200 to T-261 |
| M4 — SQL:2003 + Wire | 🔲 Not started | T-300 to T-312 |
| M5 — SQL:2008 | 🔲 Not started | T-350 to T-356 |
| M6 — SQL:2011 + SQL:2016 | 🔲 Not started | T-400 to T-430 |
| M7 — SQL:2023 Full | 🔲 Not started | T-500 to T-507 |
| M8 — Agentic Layer + v1.0 | 🔲 Not started | T-600 to T-644 |

---

## HISTORY

| Session | Date | Agent | Done | Notes |
|---------|------|-------|------|-------|
| #000 | — | Planning | Created AGENTS.md, INDEX.md, SPEC.md, TASKS.md, NOW.md | Greenfield bootstrap |
| #001 | 2026-04-18 | Codex | Completed T-001 through T-006 | Bootstrap repo, CI, lint config, Makefile, and `internal/diag` |
| #002 | 2026-04-18 | Codex | Completed T-010, T-020, T-030, and T-070 | Added native decimal-backed value system, SQL-92 keyword table, AST root contracts, and storage interface scaffold |
| #003 | 2026-04-18 | Codex | Completed T-011, T-021, T-031, T-032, and T-071 | Added type descriptors, lexer core + fuzzing, literal/name AST nodes, and the in-memory row store |
