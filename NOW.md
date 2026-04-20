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

Completed `TASKS.md` T-100 using `SPEC.md` §6, §7, §8, and §9, wiring the current Phase 1 engine into the first embeddable Go API and carrying it through focused end-to-end validation.

- Added the public `pkg/embed` surface with `Open(path)`, `DB.Exec`, `DB.Query`, `DB.Begin`, mirrored `Tx` methods, eager `ResultSet`/`CommandResult` types, and public `SQLError` diagnostics that do not leak internal packages.
- `Open(path)` now loads or creates the Phase 1 catalog metadata file, bootstraps the implicit `public` schema, and uses the in-memory row store as the backing storage engine.
- Added the first embed-side SQL execution bridge from lexer/parser/analyzer/planner into executor operators for the current SELECT subset, including `SELECT` without `FROM` via a local singleton-row operator and plan lowering for scan/filter/project.
- Added direct embed-side execution for `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, and `DROP TABLE`, plus autocommit transaction handling around `DB.Exec` and read-only autocommit around `DB.Query`.
- Explicit `Tx` support now covers query and DML work with commit/rollback, while SQL `BEGIN` / `COMMIT` / `ROLLBACK` are rejected from `Exec` in favor of the Go transaction API.
- Insert execution now rejects omitted-column cases that would require runtime `DEFAULT`, generated-column, or identity synthesis instead of guessing a value; `INSERT DEFAULT VALUES` is still reported as unsupported at execution time.
- Added `internal/storage/memory/ddl.go` so autocommit `CREATE TABLE` resets stale heaps and `DROP TABLE` removes heap state, preventing dropped tables from leaking rows into later recreations.
- Added `pkg/embed/embed_test.go` and updated `docs/api.md`, covering catalog bootstrap/reopen behavior, `SELECT 1`, end-to-end `CREATE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE`/`DROP`, transaction commit/rollback, DB-call blocking during explicit transactions, and the intentional feature-not-supported cases.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-embed go test ./pkg/embed`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-t100 go test ./pkg/embed ./internal/storage/... ./internal/catalog`.
- Verified `env GOCACHE=/tmp/tucotuco-go-test-all-t100 go test ./...`.
- Verified `env GOCACHE=/tmp/tucotuco-go-build-t100 go build ./...`.
- Confirmed again that `./.bin/` is absent and `golangci-lint` is not on `PATH`, so lint validation remains blocked on restoring a linter entrypoint in this checkout.
- Marked `TASKS.md` T-100 complete after docs, tests, and build passed.

---

## NEXT

**Task(s) to execute:** T-101 and T-102

**Instructions for the incoming agent:**

1. Read `AGENTS.md` and `INDEX.md` again before starting Phase 1 work.
2. Start **T-101** in `pkg/driver/`, mapping the new `pkg/embed` API onto `database/sql` without widening the SQL surface beyond what `pkg/embed` already executes today.
3. **T-102** in `cmd/tucotuco/` is now available in parallel and should reuse `pkg/embed` directly instead of reimplementing SQL parsing or execution in the CLI layer.
4. The current embed query bridge still inherits planner limits: `ORDER BY`, `GROUP BY`, `HAVING`, `DISTINCT`, joins, and SQL `LIMIT/OFFSET` remain unsupported and should continue returning structured diagnostics rather than partial behavior.
5. `Open(path)` still persists catalog metadata only; table rows remain in-memory until Phase 2 storage work lands, so driver/CLI messaging should not imply durable row storage yet.
6. Continue validation with writable Go caches. Lint remains blocked in this checkout because `./.bin/golangci-lint` is missing and `golangci-lint` is not on `PATH`, so either restore a linter entrypoint first or record the blocker explicitly if it is still unavailable.

**Spec references:** `SPEC.md` §6, §7, §8, §9, `SPEC_CHANGE [T-097]`

**Estimated parallelism available:** 2 streams (`T-101`, `T-102`)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | ✅ Complete | None |
| M1 — SQL-92 Core | 🟨 In progress | T-101, T-102, T-110, T-111 (`T-010`, `T-011`, `T-012`, `T-013`, `T-020`, `T-021`, `T-022`, `T-030`, `T-031`, `T-032`, `T-033`, `T-034`, `T-035`, `T-036`, `T-040`, `T-041`, `T-045`, `T-050`, `T-051`, `T-060`, `T-061`, `T-062`, `T-063`, `T-070`, `T-071`, `T-072`, `T-073`, `T-080`, `T-081`, `T-082`, `T-090`, `T-091`, `T-092`, `T-093`, `T-094`, `T-095`, `T-096`, `T-097`, `T-098`, `T-099`, `T-100`, `T-112`, `T-113` complete) |
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
| #021 | 2026-04-20 | Codex | Completed T-100 | Added the first `pkg/embed` API plus focused end-to-end tests/docs, validated repo-wide tests/build, and advanced the baton to `T-101` and `T-102` |
