# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** 2026-04-22
**Phase:** Phase 2 — SQL-92 Full Compliance + Persistent Storage
**Milestone in progress:** M2

---

## JUST DONE

Completed `TASKS.md` T-130 by adding the first SQL-92 join execution path across planner, executor, embed, CLI, and SQL-92 golden coverage.

- Parallelized the work with bounded subagents across planner discovery, executor discovery, planner implementation, executor implementation, embed integration, and fixture review.
- Added `internal/planner.Join` plus planner-side join tree construction for:
  - `INNER`, `LEFT`, `RIGHT`, `FULL`, and comma/CROSS joins
  - source-column provenance (`RelationName` + analyzer binding) on planner output columns
  - outer-join nullability propagation through joined outputs and direct projections
- Added `internal/executor/NestedLoopJoin` with:
  - left-to-right output column order
  - buffered right-side materialization
  - optional `ON` predicate evaluation over combined rows
  - NULL-extension for unmatched outer rows
- Restored a single planner-driven lowering path in `pkg/embed/lower.go` so query execution, `INSERT ... SELECT`, and CLI/compliance harnesses all share the same join-aware row-shape/binding logic.
- Updated focused and golden coverage:
  - planner join shape/explain/nullability/alias tests
  - executor nested-loop join lifecycle and semantic tests
  - embed end-to-end join tests plus SQL-92 fixture flips for derived joins and comma/CROSS joins
  - explicit unsupported diagnostics retained for `JOIN ... USING` and `NATURAL JOIN` in this baton
- Revalidated focused join coverage:
  - `env GOCACHE=/tmp/tucotuco-go-test-core-t130 go test ./internal/planner ./pkg/embed ./cmd/tucotuco ./compliance/sql92`
  - `env GOCACHE=/tmp/tucotuco-go-test-executor-t130 go test ./internal/executor`
  - `git diff --check`
- Revalidated repo-wide regression/build/lint coverage:
  - `env GOCACHE=/tmp/tucotuco-go-test-all-t130b go test ./...`
  - `env GOCACHE=/tmp/tucotuco-go-build-all-t130b go build ./...`
  - `env XDG_CACHE_HOME=/tmp/tucotuco-xdg-cache-all-t130b GOCACHE=/tmp/tucotuco-go-lint-all-t130b golangci-lint run ./...`

---

## NEXT

**Task(s) to execute:** T-131

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting the next SQL task.
2. Start **T-131** as the first unchecked task in `TASKS.md`: add subquery support (`scalar`, `EXISTS`, `IN`, correlated) on top of the new join-capable planner/executor path.
3. Keep the new storage contract untouched unless subquery execution exposes a real storage regression:
  - `internal/storage/storagetest/contract.go` is now the shared storage baseline
  - committed paged handle-stability expectations still live in the paged relation/vacuum tests
4. Read the subquery semantics in `SPEC.md` §8.1 and especially §8.2 before editing analyzer, planner, or executor code.
5. The join baseline now exists in:
  - `internal/planner/plan.go` / `internal/planner/builder.go`
  - `internal/executor/join.go`
  - `pkg/embed/lower.go`
  - `testdata/results/045_*.txt`, `050_multiple_from_sources.txt`, `057_*.txt`, and `059_*.txt`
6. `JOIN ... USING` and `NATURAL JOIN` are still explicit feature errors after `T-130`; do not silently broaden them while working on `T-131`.

**Spec references:** `SPEC.md` §8.2

**Estimated parallelism available:** 6 streams after the first `T-131` planning pass

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | ✅ Complete | None (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-095`, `T-096`, `T-097`, `T-098`, `T-099`, `T-100`, `T-101`, `T-102`, `T-110`, `T-111`, `T-112`, `T-113` complete) |
| M2 — SQL-92 Full + Storage | 🟨 In progress | T-128 to T-171 (`T-120`, `T-121`, `T-122`, `T-123`, `T-124`, `T-125`, `T-126`, `T-127` complete) |
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
