# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** 2026-04-25
**Phase:** Phase 2 — SQL-92 Full Compliance + Persistent Storage
**Milestone in progress:** M2

---

## JUST DONE

Completed `TASKS.md` T-140 and T-141 by landing logical views and logical-plan `EXPLAIN` across parser, catalog, analyzer, executor, embed, script routing, and SQL-92 goldens.

- Used bounded subagents for T-140 implementation, T-141 implementation, and read-only T-140 review; fixed the review finding so writes to views consistently reach the intended `0A000` feature error instead of analyzer write-shape/nullability diagnostics.
- Added parser CST support for:
  - `CREATE VIEW name [(columns)] AS <query> [WITH [CASCADED|LOCAL] CHECK OPTION]`
  - `DROP VIEW name`
  - `EXPLAIN <query-expression>`
  - `EXPLAIN ANALYZE <query-expression>` as an explicit feature error.
- Added catalog `ViewDescriptor` storage, defensive-copy APIs, table/view name collision checks, schema non-empty handling for views, and JSON catalog persistence for logical views.
- Added analyzer resolution/type output support for views as logical catalog relations while preserving table/view write rejection through embed.
- Added executor-native `CreateView` / `DropView` DDL operators and embed execution/persistence wiring.
- Added runtime expansion for logical views during query lowering; views are always computed from the stored SQL query at read time.
- Added `EXPLAIN` query execution using `internal/planner.Explain`, returning one `query_plan VARCHAR` column with one row per rendered plan line; script dispatch now routes `EXPLAIN` through `Query`.
- Added SQL-92 golden fixtures:
  - `074_view_ddl`
  - `075_explain`
- Revalidated focused coverage:
  - `env GOCACHE=/tmp/tucotuco-go-test-t140-t141-focused go test ./internal/parser ./internal/catalog ./internal/analyzer ./internal/executor ./internal/script ./pkg/embed ./compliance/sql92 -count=1`
  - `git diff --check`
- Revalidated repo-wide regression/build/lint coverage:
  - `env GOCACHE=/tmp/tucotuco-go-test-t140-t141-all go test ./...`
  - `env GOCACHE=/tmp/tucotuco-go-build-t140-t141 go build ./...` (exited 0; Go emitted a non-fatal stat-cache warning from the read-only module cache)
  - `env XDG_CACHE_HOME=/tmp/tucotuco-xdg-cache-t140-t141 GOCACHE=/tmp/tucotuco-go-lint-t140-t141 /tmp/tucotuco-bin/golangci-lint run ./...`

---

## NEXT

**Task(s) to execute:** T-150 and T-151 first; plan T-152 after the write-constraint pass is understood.

**Instructions for the incoming agent:**

1. Read `AGENTS.md`, `INDEX.md`, `TASKS.md`, and the relevant `SPEC.md` sections before starting the constraint slice.
2. Start **T-150** (`NOT NULL enforcement at write time`) and **T-151** (`CHECK constraint expression evaluation`) as the first unchecked parallel-safe tasks in `TASKS.md`.
3. Treat **T-152** (`PRIMARY KEY constraint + unique enforcement`) as unblocked but likely conflict-prone with T-150/T-151 because all three may touch write execution, catalog constraint descriptors, analyzer write checks, and embed DML paths. Do a first planning pass before running it concurrently.
4. Preserve the new DDL/query-object baseline while working forward:
  - `CREATE SCHEMA` / `DROP SCHEMA`, schema-qualified table/column names, logical `CREATE VIEW` / `DROP VIEW`, and `EXPLAIN` are now covered by SQL-92 goldens `072` through `075`.
  - `DROP SCHEMA CASCADE` remains a feature error.
  - Writes to views must return `0A000` feature errors before table write semantics run.
  - Views are logical and computed from stored SQL at query time; do not add materialized-view behavior in M2.
5. Likely T-150/T-151 seams:
  - analyzer write validation in `internal/analyzer/writecheck.go` / `typecheck.go`
  - executor DML operators in `internal/executor/dml.go`
  - embed DML descriptor/mapping paths in `pkg/embed/dml_ddl.go`
  - catalog constraint metadata in `internal/catalog/`
  - SQL-92 golden fixtures under `testdata/queries` / `testdata/results`
6. `JOIN ... USING` and `NATURAL JOIN` remain explicit feature errors after `T-130`, `SIMILAR TO` remains unowned beyond `T-136`, `DROP SCHEMA CASCADE` remains unsupported, and `EXPLAIN ANALYZE` remains a feature error until `T-239`; do not broaden these incidentally.

**Spec references:** `SPEC.md` §11 (Constraints), plus `SPEC.md` §6.1 for table constraint syntax/metadata and §7 for DML write semantics

**Estimated parallelism available:** 2-3 streams after the first constraints planning pass; keep `internal/catalog/`, shared DML write execution, and `go.mod` serial

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | ✅ Complete | None (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-095`, `T-096`, `T-097`, `T-098`, `T-099`, `T-100`, `T-101`, `T-102`, `T-110`, `T-111`, `T-112`, `T-113` complete) |
| M2 — SQL-92 Full + Storage | 🟨 In progress | T-150 to T-171 (`T-120` to `T-128`, `T-130` to `T-141` complete) |
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
| #028 | 2026-04-21 | Codex | Completed T-122 | Added the first relation-local heap file manager in `internal/storage/paged` with page-0 metadata, per-table relation files, schema-driven tuple encoding, handle routing, focused relation tests, and repo-wide green validation, then advanced the baton to `T-123` |
| #029 | 2026-04-21 | Codex | Completed T-123 | Added the first file-backed WAL layer in `internal/wal` plus paged-storage WAL emission, page-LSN stamping, WAL-before-page-flush ordering, focused WAL/paged tests, and repo-wide green validation, then advanced the baton to `T-124` |
| #030 | 2026-04-21 | Codex | Completed T-124 | Parallelized `T-124` with six bounded subagents, added restart-time WAL redo before paged-storage validation, extended WAL scan helpers plus recovery tests, passed focused and repo-wide tests/build, and advanced the baton to `T-125` |
| #031 | 2026-04-21 | Codex | Completed T-125 | Parallelized `T-125` with bounded subagents, activated relation-local tuple version metadata (`xmin` / `xmax`), persisted page-0 version allocation, added version-aware paged-storage + recovery tests, passed focused and repo-wide tests/build, and advanced the baton to `T-126` |
| #032 | 2026-04-21 | Codex | Completed T-126 | Finished relation-local begin/commit/rollback in `internal/storage/paged`, fixed redirect/version-floor reopen regressions, added transaction visibility/rollback durability tests, passed focused and repo-wide tests/build, and advanced the baton to `T-127` |
| #033 | 2026-04-22 | Codex | Completed T-127 | Parallelized vacuum implementation/coverage/lint closeout, added `Relation.Vacuum()` plus paged reclamation tests, restored repo-wide `golangci-lint` validation, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-128` |
| #034 | 2026-04-22 | Codex | Completed T-128 | Parallelized the storage-contract migration with five bounded subagents, extracted a shared storage behavior suite under `internal/storage/storagetest`, adopted it in both memory and paged storage, restored the memory-specific reserved-handle regression, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-130` |
| #035 | 2026-04-22 | Codex | Completed T-130 | Added logical/planner/executor/embed support for `INNER`/`LEFT`/`RIGHT`/`FULL`/comma-CROSS joins, preserved joined-column provenance and outer-join nullability through lowering, updated SQL-92 join/comma fixtures plus explicit `USING`/`NATURAL` feature errors, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-131` |
| #036 | 2026-04-22 | Codex | Completed T-131 | Added parser/analyzer/runtime support for scalar/`EXISTS`/`IN`/correlated subqueries, extended embed lowering with subquery execution and scalar-cardinality diagnostics, added SQL-92 fixtures `061_064`, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-132` |
| #037 | 2026-04-22 | Codex | Completed T-132 through T-135 | Added end-to-end `UNION`/`INTERSECT`/`EXCEPT` query expressions plus SQL-92 fixtures `065_068` for CASE, LIKE, NULL logic, and set operations, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-136` |
| #038 | 2026-04-22 | Codex | Completed T-136 and T-137 | Added SQL-standard scalar-function coverage plus grouped/global aggregate query support, refreshed SQL-92 fixtures `069_071` and the older aggregate planner-limit transcripts, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-138` |
| #039 | 2026-04-25 | Codex | Completed T-138 and T-139 | Added schema DDL plus schema-qualified name resolution, refreshed SQL-92 fixtures `072_073`, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-140` |
| #040 | 2026-04-25 | Codex | Completed T-140 and T-141 | Added logical views plus logical-plan `EXPLAIN`, refreshed SQL-92 fixtures `074_075`, fixed view-write diagnostics, passed focused and repo-wide tests/build/lint, and advanced the baton to `T-150` / `T-151` |
