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

Completed `TASKS.md` T-080 and T-081 using `SPEC.md` §7 and §8, establishing the first logical planner contracts plus the initial analyzed-`SELECT` builder slice.

- Added `internal/planner/plan.go` with the `Plan` interface, `Kind` enum, output `Column` metadata, `Projection`, and the basic logical nodes `Scan`, `Filter`, `Project`, and `Limit`.
- Added `internal/planner/builder.go` with a `Builder` that translates analyzed `SELECT` statements into logical plans over `Scan`, `Filter`, and `Project`, recurses through derived-table subqueries, and emits planner diagnostics for unsupported Phase 1 query features such as `ORDER BY`, `GROUP BY`, `DISTINCT`, joins, and aggregate queries.
- Kept planner output metadata sourced from analyzer `Bindings` and `Types`, including star expansion through analyzer side tables instead of re-resolving names or types inside the planner.
- Added `internal/planner/plan_test.go` and `internal/planner/builder_test.go` covering node construction, output-schema propagation, defensive slice behavior, star expansion, derived-table planning, and unsupported query diagnostics.
- Restored a repo-local `golangci-lint` binary at `./.bin/golangci-lint` after the prior session's missing-tool blocker.
- Verified `gofmt -w internal/planner/*.go`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go test ./internal/planner`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go test ./...`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build GOLANGCI_LINT_CACHE=/tmp/tucotuco-golangci-lint-cache ./.bin/golangci-lint run ./internal/planner`.
- Observed that `env GOCACHE=/tmp/tucotuco-go-build GOLANGCI_LINT_CACHE=/tmp/tucotuco-golangci-lint-cache ./.bin/golangci-lint run ./...` currently reports `no go files to analyze`; targeted package lint succeeded, but future sessions should prefer explicit package paths until the repo-wide invocation is understood.
- Marked `TASKS.md` T-080 and T-081 complete after planner tests and targeted lint passed.

---

## NEXT

**Task(s) to execute:** T-082

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Start **T-082** in `internal/planner/`, reusing `Plan.String()` for node summaries rather than inventing a second label format for EXPLAIN output.
3. Keep the printer aligned with the current tree shape from `builder.go`: preorder or indented tree output over `Project`, `Filter`, `Scan`, and later `Limit` nodes, with stable formatting suitable for golden tests.
4. Add focused planner tests that build analyzed `SELECT` plans through the real builder and then assert EXPLAIN output strings, especially for derived-table nesting and unsupported-feature diagnostics not leaking into printer code.
5. Continue using analyzer `Bindings` and `Types` only through the built plan; the printer should not re-open analyzer semantics once a plan exists.
6. Use the restored repo-local linter with writable caches for validation: `env GOCACHE=/tmp/tucotuco-go-build GOLANGCI_LINT_CACHE=/tmp/tucotuco-golangci-lint-cache ./.bin/golangci-lint run <pkg>`.
7. `T-090` is now also unblocked, but `T-082` remains the first unblocked task in `TASKS.md`; update `NOW.md` accordingly if you choose to branch into executor work on a second stream.

**Spec references:** `SPEC.md` §7, §8

**Estimated parallelism available:** 2 streams (`T-082`, `T-090`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-037 to T-113 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-112` complete) |
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
