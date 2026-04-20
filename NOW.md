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

Completed `TASKS.md` T-092, T-093, and T-096 using `SPEC.md` §7 and §8, parallelizing the current executor frontier across three bounded subagents on top of the new compiled-expression seam.

- Added `internal/executor/filter.go` and `internal/executor/filter_test.go`, implementing a row-preserving `Filter` operator that evaluates one compiled predicate per row, passes only SQL `TRUE`, rejects both `FALSE` and `NULL`, mirrors the shared lifecycle contract, preserves child handles exactly, and rolls back its own `Open` state when the child fails to open.
- Added `internal/executor/project.go` and `internal/executor/project_test.go`, implementing a row-preserving `Project` operator that evaluates ordered compiled expressions into a fresh output value slice per row, preserves child handles unchanged, and mirrors the same lifecycle and open-failure rollback semantics.
- Added `internal/executor/limit.go` and `internal/executor/limit_test.go`, implementing an executor-native `Limit` operator with forward-compatible offset support via `NewLimitWithOffset`, stable repeated `io.EOF`, child-handle preservation, and the same open-failure rollback pattern.
- Focused executor coverage now includes lifecycle rules, repeated `io.EOF`, open-failure retry behavior, child close propagation, row-handle preservation, filter truth-table behavior, projection ordering and fresh-row allocation, and limit/offset truncation behavior.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-executor go test ./internal/executor`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-all go test ./...`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build-all go build ./...`.
- Confirmed again that `./.bin/` is absent and `golangci-lint` is not on `PATH` in this checkout, so lint validation remains blocked on restoring the repo-local binary or providing another linter entrypoint.
- Marked `TASKS.md` T-092, T-093, and T-096 complete after tests and build passed.

---

## NEXT

**Task(s) to execute:** T-095, T-097, T-098, T-099

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Start **T-095** first in `internal/executor/`, keeping it executor-native on top of compiled evaluator callbacks and existing row semantics; current planner code still rejects `ORDER BY`, so keep planner diagnostics and EXPLAIN changes out of that patch.
3. **T-097** is now unblocked but more contract-sensitive than the just-finished row-preserving operators; keep its interface explicit and local to `internal/executor/`, and if `SPEC.md` §8 leaves any aggregate-state detail ambiguous, stop and raise `SPEC_QUESTION` instead of guessing.
4. **T-098** and **T-099** are also runnable. Keep both executor-facing: consume analyzed or compiled write inputs plus catalog or storage handles without folding public API wiring or planner-side name resolution into the same batch.
5. Mirror the shared executor lifecycle semantics from `internal/executor/executor.go` and the `SeqScan` open-failure rollback pattern from `internal/executor/seqscan.go` anywhere a child operator or lower-level dependency must be opened first.
6. Continue validation with writable Go caches. Lint remains blocked in this checkout because `./.bin/golangci-lint` is missing and `golangci-lint` is not on `PATH`, so either restore a linter entrypoint first or record the blocker explicitly if it is still unavailable.

**Spec references:** `SPEC.md` §7, §8

**Estimated parallelism available:** 4 streams (`T-095`, `T-097`, `T-098`, `T-099`; coordinate carefully across `internal/executor/` plus any `catalog` or `storage` touchpoints)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-037 to T-113 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-096`, `T-112` complete) |
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
