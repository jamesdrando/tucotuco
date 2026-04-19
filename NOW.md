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

Completed `TASKS.md` T-061 using `SPEC.md` §3, §5, §7, and §8.

- Added a side-table-based analyzer type checker in `internal/analyzer/types.go` and `internal/analyzer/typecheck.go` that assigns types to the current Phase 1 parser-local CST surface, reuses resolver bindings for catalog/derived/local column references, normalizes parser type names for `CAST` and `CREATE TABLE`, and applies boolean/assignment/default context checks without mutating CST nodes.
- Added `internal/analyzer/typecheck_test.go` with focused coverage for select outputs, alias/derived-table propagation, `COUNT`/`COALESCE`/`CASE`/`CAST`, contextual `NULL` typing, `CREATE TABLE` defaults, and representative `42804`/`42883` diagnostics.
- Verified `env GOCACHE=/tmp/tucotuco-go-build go test ./internal/analyzer`.
- Verified `env GOLANGCI_LINT_CACHE=/tmp/golangci-lint-cache GOCACHE=/tmp/tucotuco-go-build ./.bin/golangci-lint run ./internal/analyzer`.
- Marked `TASKS.md` T-061 complete after validation passed.

---

## NEXT

**Task(s) to execute:** T-062, T-063

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Read the semantic-analysis context for **T-062** and **T-063**, and build directly on the current `internal/analyzer` resolver/type-checker state rather than reworking name or type assignment.
3. Keep the work confined to `internal/analyzer/`; `T-062` should validate column-count / assignment-shape / `NOT NULL` constraints, while `T-063` should validate aggregate and window placement over the already typed CST.
4. Reuse the analyzer side tables for bindings and types instead of writing semantic data back into parser nodes.
5. Add focused analyzer tests for successful validation and representative diagnostics for both tasks.
6. Run targeted validation for `internal/analyzer`, then mark `TASKS.md` `T-062` and/or `T-063` complete as each task goes green.
7. Update `TASKS.md`, `NOW.md`, and `SPEC.md` if later design decisions diverge from the current spec text.

**Spec references:** `SPEC.md` §3, §5, §7, §8

**Estimated parallelism available:** 2 streams (`T-062`, `T-063`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-037 to T-113 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-070`, `T-071`, `T-072`, `T-073`, `T-112` complete) |
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
