# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** 2026-04-20
**Phase:** Phase 1 — Core Engine (In-Memory, SQL-92 Subset)
**Milestone in progress:** M1

---

## JUST DONE

Completed `TASKS.md` T-121 by landing the first concrete buffer-pool layer in `internal/storage/paged/` on top of the T-120 page-layout contract.

- Added `internal/storage/paged/page.go` with a 64-byte page-header codec, checksum handling, page initialization, and header validation aligned with `docs/storage.md`.
- Added `internal/storage/paged/store.go` with a `PageStore` abstraction and a file-backed `FileStore` that reserves page `0` for relation metadata and exposes page-level read/write/allocate behavior.
- Added `internal/storage/paged/manager.go` with a `Manager` buffer pool that implements:
  - page fetch and new-page allocation
  - pin/unpin lifecycle
  - dirty-page tracking
  - explicit flush / flush-all
  - LRU eviction over unpinned frames
  - stale-page protection via internal tokens
- Added focused tests in `internal/storage/paged/store_test.go` and `internal/storage/paged/manager_test.go` covering page initialization/validation, corruption detection, dirty eviction/writeback, and cache-full pinned-frame behavior.
- Verified focused validation with a writable Go cache:
  - `env GOCACHE=/tmp/tucotuco-go-test-t121-focused go test ./internal/storage/paged/...`
- Verified repo-wide regression/build coverage:
  - `env GOCACHE=/tmp/tucotuco-go-test-all-t121 go test ./...`
  - `env GOCACHE=/tmp/tucotuco-go-build-t121 go build ./...`
- Lint remains environment-blocked unless a `golangci-lint` entrypoint is restored in this checkout.
- Marked `TASKS.md` T-121 complete after focused and repo-wide validation passed.

---

## NEXT

**Task(s) to execute:** T-122

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting the next storage task.
2. Start **T-122** by building the heap file manager on top of the new paged buffer pool: relation-file layout, table-to-page mapping, page-0 metadata usage, page selection for inserts, and row routing to page/slot handles.
3. Keep the task boundary sharp: T-122 should consume the buffer pool and page format, but not absorb WAL durability rules from T-123 or MVCC visibility from T-125.
4. Use `docs/storage.md` and the new `internal/storage/paged/` types as the contract. Prefer adding small internal helpers under `internal/storage/paged/` or a neighboring heap-file package rather than redesigning the buffer-pool API.
5. Preserve the existing `internal/storage` public interfaces unless the heap-file implementation needs small compatible extensions.
6. Lint remains environment-blocked unless a `golangci-lint` entrypoint is restored in this checkout.

**Spec references:** `SPEC.md` §6, §7, §8, §9, `SPEC_CHANGE [T-097]`

**Estimated parallelism available:** 1 stream (`T-122` is serial)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | ✅ Complete | None (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-095`, `T-096`, `T-097`, `T-098`, `T-099`, `T-100`, `T-101`, `T-102`, `T-110`, `T-111`, `T-112`, `T-113` complete) |
| M2 — SQL-92 Full + Storage | 🟨 In progress | T-122 to T-171 (`T-120`, `T-121` complete) |
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
| #004 | 2026-04-18 | Codex | Completed T-012, T-022, T-033, T-040, T-050, T-072, T-073, and T-112 | Added coercion rules, SQL:1999 keyword metadata, unary/binary AST nodes, parser scaffold, in-memory catalog, scan/isolation upgrades, and dedicated CI race coverage |
| #005 | 2026-04-18 | Codex | Completed T-013, T-034, T-035, T-036, T-041, T-045, and T-051 | Added runtime casts, refined statement AST shapes, parser-local CST expression/TCL parsing with fuzz coverage, and forward-compatible catalog descriptors plus follow-up validation fixes |
| #006 | 2026-04-18 | Codex | Cleared current lint backlog | Installed repo-local `golangci-lint`, fixed the 8 reported warnings in lexer/parser/types/storage, and verified `golangci-lint run ./...` plus focused package tests |
| #007 | 2026-04-18 | Codex | Implemented T-037, T-042, T-043, T-044, and T-052 (pending validation) | Added AST pretty-printing, parser-local CST statement parsing for Phase 1 DML/DDL, and versioned JSON catalog persistence via subagent parallelization; validation intentionally deferred |
| #008 | 2026-04-18 | Codex | Completed T-037, T-042, T-043, T-044, and T-052 | Fixed closeout compile/lint fallout, validated `internal/ast`, `internal/parser`, and `internal/catalog`, and advanced the baton to `T-046` and `T-060` |
| #009 | 2026-04-18 | Codex | Completed T-046; started T-060 | Extended script parsing to ignore empty semicolon-only segments, validated the parser package, and moved the baton to analyzer name resolution |
| #010 | 2026-04-18 | Codex | Completed T-060 | Added and validated the first analyzer resolver over the parser-local CST, then advanced the baton to `T-061` |
| #011 | 2026-04-18 | Codex | Completed T-061 | Added the first analyzer type-check pass with CST type-name normalization, focused semantic diagnostics/tests, and targeted analyzer test/lint validation, then advanced the baton to `T-062` and `T-063` |
| #012 | 2026-04-18 | Codex | Completed T-062 and T-063 | Added analyzer-side write validation plus aggregate/grouped-query placement checks, validated `internal/analyzer` with focused package tests, and moved the baton to planner task `T-080` |
| #013 | 2026-04-19 | Codex | Completed T-080 and T-081 | Added the first logical planner node contracts plus the initial analyzed-SELECT builder, restored the repo-local linter binary, validated planner lint/tests, and advanced the baton to `T-082` |
| #014 | 2026-04-19 | Codex | Completed T-082 and T-090 | Added the logical EXPLAIN-style plan printer plus the initial executor operator contract, validated planner/executor and repo-wide tests/build, and advanced the baton to `T-091` with lint still blocked by the missing `golangci-lint` binary |
| #015 | 2026-04-19 | Codex | Completed T-091 | Added the first executor-side sequential scan operator with focused lifecycle/row/option tests, validated executor/storage and repo-wide tests/build, and advanced the baton to `T-094` because `T-092`/`T-093` need the expression-evaluator contract first |
| #016 | 2026-04-20 | Codex | Completed T-094 | Added the executor-side compiled expression evaluator plus focused runtime tests, validated repo-wide tests/build, and advanced the baton to `T-092` and `T-093` on top of the new compiled-expression seam |
| #017 | 2026-04-20 | Codex | Completed T-092, T-093, and T-096 | Parallelized the current executor frontier with bounded subagents, added row-preserving filter/project operators plus limit/offset support, validated executor and repo-wide tests/build, and advanced the baton to `T-095`, `T-097`, `T-098`, and `T-099` |
| #018 | 2026-04-20 | Codex | Completed T-095, T-098, and T-099; blocked T-097 | Parallelized the executor frontier with bounded subagents, added stable sort plus executor-local DML/DDL operators, validated executor and repo-wide tests/build, and raised `SPEC_QUESTION [T-097]` instead of guessing on aggregate semantics |
| #019 | 2026-04-20 | Codex | Completed T-097 | Resolved the aggregate semantics toward PostgreSQL behavior, added executor-native hash aggregation with focused tests, validated executor and repo-wide tests/build, and advanced the baton to `T-100` and `T-113` |
| #020 | 2026-04-20 | Codex | Completed T-113 | Added the missing executor fuzz target plus parser script fuzz coverage, validated the focused lexer/parser/executor packages, and narrowed the baton to `T-100` |
| #021 | 2026-04-20 | Codex | Completed T-100 | Added the first `pkg/embed` API plus focused end-to-end tests/docs, validated repo-wide tests/build, and advanced the baton to `T-101` and `T-102` |
| #022 | 2026-04-20 | Codex | Implemented T-101 and T-102 (validation pending) | Used six bounded subagents to land the Phase 1 `database/sql` driver plus CLI code/tests/docs, corrected connector sharing to avoid cross-`sql.DB` state leakage, isolated CLI tests from source-tree default-path writes, and left validation/lint as the explicit next step |
| #023 | 2026-04-20 | Codex | Completed T-101 and T-102 | Validated `pkg/driver` and `cmd/tucotuco`, fixed driver transaction routing through active `embed.Tx`, passed repo-wide tests/build, recorded lint as environment-blocked, and advanced the baton to `T-110` |
| #024 | 2026-04-20 | Codex | Completed T-110 | Added `internal/script`, refactored the CLI and SQL-92 golden harness onto the shared script seam, seeded deterministic `.sql` / `.txt` fixtures, fixed closeout regressions found by validation, passed focused and repo-wide tests/build, and advanced the baton to `T-111` |
| #025 | 2026-04-20 | Codex | Completed T-111 | Expanded `compliance/sql92` to 50+ deterministic cases, fixed validation-driven fixture drift across supported and unsupported paths, passed focused and repo-wide tests/build, closed M1, and advanced the baton to `T-120` |
| #026 | 2026-04-20 | Codex | Completed T-120 | Replaced the storage stub with a concrete paged-layout design in `docs/storage.md`, aligned the document with the current `RowHandle` API and future MVCC/WAL needs, and advanced the baton to `T-121` |
| #027 | 2026-04-20 | Codex | Completed T-121 | Added the first `internal/storage/paged` buffer-pool layer with validated page headers, file-backed page storage, LRU-managed frames, focused paged-storage tests, and repo-wide green validation, then advanced the baton to `T-122` |
