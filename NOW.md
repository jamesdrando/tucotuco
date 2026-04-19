# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** 2026-04-19
**Phase:** Phase 1 — Core Engine (In-Memory, SQL-92 Subset)
**Milestone in progress:** M1

---

## JUST DONE

Completed `TASKS.md` T-091 using `SPEC.md` §8, adding the first concrete physical scan operator on top of the new executor contract and the existing storage scan path.

- Added `internal/executor/seqscan.go` with the `SeqScan` operator over `storage.Storage`, `storage.Transaction`, and `storage.RowIterator`, reusing executor lifecycle semantics and bridging rows through `NewRowFromStorage`.
- Added `internal/executor/seqscan_test.go` covering lifecycle misuse, repeated `io.EOF`, handle-preserving row conversion, scan-option forwarding, and failed `Open()` behavior leaving the operator not-open while keeping `Close()` safe.
- Confirmed through review that `T-092` and `T-093` should not yet evaluate raw planner/parser expressions directly; the current executor shape needs the compiled expression contract from `T-094` first to avoid duplicating or inventing the wrong runtime binding model.
- Verified `gofmt -w internal/executor/seqscan.go internal/executor/seqscan_test.go`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go test ./internal/executor ./internal/storage ./internal/storage/memory`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go test ./...`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go build ./...`.
- Confirmed again that `./.bin/` is absent and `golangci-lint` is not on `PATH` in this checkout, so lint validation remains blocked on restoring the repo-local binary or providing another linter entrypoint.
- Marked `TASKS.md` T-091 complete after tests and build passed.

---

## NEXT

**Task(s) to execute:** T-094

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Start **T-094** in `internal/executor/`, defining the execution-time expression evaluator contract that can consume planner/parser expressions against executor rows without reintroducing analyzer/planner coupling at runtime.
3. Treat `T-092` and `T-093` as effectively downstream of that evaluator contract even though `TASKS.md` does not currently encode the dependency; direct raw-`parser.Node` evaluation inside `Filter` or `Project` would duplicate or pre-empt `T-094`.
4. Keep the evaluator aligned with the current executor row model from `internal/executor/executor.go` and the new `SeqScan` output shape from `internal/executor/seqscan.go`; later `Filter` and `Project` operators should consume compiled callbacks or equivalent executor-native evaluation primitives, not planner-side helpers.
5. Add focused tests for scalar evaluation over executor rows, beginning with the current Phase 1 surface used by planner output: identifiers bound by ordinal position, literals, unary/binary operators, boolean predicates, and `CAST`/`TRY_CAST` reuse from the existing type runtime.
6. Continue treating planner diagnostics and EXPLAIN rendering as separate concerns; executor work should consume already-analyzed plans and row values, not planner errors.
7. Validation should continue using writable Go caches. Lint is currently blocked in this checkout because `./.bin/golangci-lint` is missing, so either restore the repo-local binary first or record the blocker explicitly if lint is still unavailable.

**Spec references:** `SPEC.md` §7, §8

**Estimated parallelism available:** 3 streams (`T-094`, design prep for `T-092`, design prep for `T-093`; coordinate carefully inside `internal/executor/`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-037 to T-113 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-112` complete) |
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
