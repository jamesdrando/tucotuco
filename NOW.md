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

Completed `TASKS.md` T-097 and T-113 using `SPEC.md` §8 plus the existing lexer/parser/executor contracts, resolving aggregate semantics toward PostgreSQL behavior and closing the remaining Phase 1 fuzz-target gap.

- Added `internal/executor/hash_aggregate.go` and `internal/executor/hash_aggregate_test.go`, implementing an executor-native blocking `HashAggregate` operator with explicit local group-key and aggregate specs, first-`Next()` materialization, stable repeated `io.EOF`, retryable child-open failures, and synthetic output rows with zero storage handles.
- Implemented grouped and global `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `EVERY` over compiled expression callbacks, with PostgreSQL-style empty-input and all-`NULL` behavior: `COUNT` returns `0`, the others return `NULL`, and `EVERY` ignores `NULL` inputs and otherwise follows PostgreSQL `bool_and` semantics.
- Exact-numeric `AVG` now computes an exact rational and converts it to `NUMERIC` with 16 fractional digits before the repo's Decimal normalization step; approximate `AVG` returns `DOUBLE PRECISION`.
- Resolved `SPEC_QUESTION [T-097]` by replacing it with `SPEC_CHANGE [T-097]` in `SPEC.md`, explicitly recording the PostgreSQL-first aggregate semantics and the Phase 1 exact-numeric `AVG` rule.
- Added `internal/executor/fuzz_test.go`, introducing a dedicated executor expression compile/evaluate fuzz target over the current literal-heavy Phase 1 evaluator surface.
- Extended `internal/parser/fuzz_test.go` with `FuzzParseScript`, covering multi-statement script parsing on top of the existing expression fuzz target.
- Focused executor coverage now also includes grouped/global aggregate behavior, empty-input semantics, all-`NULL` group semantics, approximate vs exact `AVG`, signed-zero group-key normalization, repeated `io.EOF`, materialization-time expression failures, child close propagation, and retryable open failures for `HashAggregate`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-executor go test ./internal/executor`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-fuzz go test ./internal/lexer ./internal/parser ./internal/executor`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-all go test ./...`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build-all go build ./...`.
- Confirmed again that `./.bin/` is absent and `golangci-lint` is not on `PATH` in this checkout, so lint validation remains blocked on restoring the repo-local binary or providing another linter entrypoint.
- Marked `TASKS.md` T-097 and T-113 complete after tests and build passed.

---

## NEXT

**Task(s) to execute:** T-100

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Start **T-100** first in `pkg/embed/`, wiring the now-complete Phase 1 executor surface (`T-098`, `T-099`, `T-082`) into a minimal embeddable API without mixing in `database/sql` driver registration or CLI work from later tasks.
3. Aggregate semantics are now pinned in `SPEC_CHANGE [T-097]`; future planner or API wiring for grouped queries should follow that note instead of introducing new aggregate result rules ad hoc.
4. Continue mirroring the shared executor lifecycle semantics from `internal/executor/executor.go` and the `SeqScan` open-failure rollback pattern from `internal/executor/seqscan.go` anywhere a child operator or lower-level dependency must be opened first.
5. Continue validation with writable Go caches. Lint remains blocked in this checkout because `./.bin/golangci-lint` is missing and `golangci-lint` is not on `PATH`, so either restore a linter entrypoint first or record the blocker explicitly if it is still unavailable.

**Spec references:** `SPEC.md` §6, §7, §8, `SPEC_CHANGE [T-097]`

**Estimated parallelism available:** 1 stream (`T-100`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-037 to T-113 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-095`, `T-096`, `T-097`, `T-098`, `T-099`, `T-112`, `T-113` complete) |
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
