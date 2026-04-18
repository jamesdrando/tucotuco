# NOW.md — Agent Handoff Log

> **Protocol:** Every agent session reads this file first and writes to it last.
> - Move "NEXT" → "JUST DONE" before starting work.
> - Write the new "NEXT" based on the first unblocked task in TASKS.md.
> - Reference SPEC.md sections and TASKS.md IDs explicitly.
> - Keep entries concise and factual — this is a baton, not a diary.

---

## CURRENT STATE

**Date:** _not started_
**Phase:** Phase 0 — Repo Bootstrap
**Milestone in progress:** M0

---

## JUST DONE

_Nothing yet. This is a greenfield project._

---

## NEXT

**Task(s) to execute:** T-001, T-003, T-004 (T-001 is blocking; T-003 and T-004 can start in parallel once T-001 completes)

**Instructions for the incoming agent:**

1. Read `AGENTS.md` for the collaboration contract.
2. Read `INDEX.md` for the intended directory layout.
3. Read `SPEC.md §1` and `SPEC.md §2` for project goals and compatibility policy.
4. Execute **T-001**: Initialise the Go module.
   - Module path: `github.com/yourorg/tucotuco` (confirm with project owner if different).
   - Go version: `1.22` minimum.
   - Create root `doc.go` with package-level comment summarising the project.
   - Acceptance: `go build ./...` succeeds.
5. Execute **T-002**: Scaffold the full directory layout from `INDEX.md`. Each package needs only a `doc.go` stub with a one-line package comment.
6. Once T-001 and T-002 are green, spawn two parallel subagents:
   - **Subagent A** → T-003 (CI pipeline)
   - **Subagent B** → T-004 (golangci-lint config)
7. After both return, execute **T-005** (Makefile) and **T-006** (`internal/diag` package).
8. Mark T-001 through T-006 complete in `TASKS.md`.
9. Update this file: move these instructions to JUST DONE, write the Phase 1 kickoff as NEXT.

**Spec references:** SPEC.md §1, §2, §19 (error model — needed for `internal/diag`)

**Estimated parallelism available:** 2 subagents (T-003 + T-004 after T-001/T-002 land)

---

## BLOCKED

_None._

---

## MILESTONE TRACKER

| Milestone | Status | Tasks remaining |
|-----------|--------|----------------|
| M0 — Repo Ready | 🔲 Not started | T-001 to T-006 |
| M1 — SQL-92 Core | 🔲 Not started | T-010 to T-113 |
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
