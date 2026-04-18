# AGENTS.md — tucotuco Agent Collaboration Contract

> **tucotuco** is a pure-Go SQL engine targeting full SQL:2023 compliance, native MCP integration, and a pluggable skills/integrations layer.
> This file defines the operating contract for every AI agent (human or automated) that works on this codebase.

---

## 1. Guiding Principles

| Principle | Meaning in practice |
|-----------|---------------------|
| **KISS** | Every abstraction must justify its existence. Prefer flat over nested. |
| **DRY** | One source of truth. Generate repetitive code; never copy-paste it. |
| **SOLID** | Interfaces over concrete types. Dependency injection everywhere. |
| **Production-first** | No `TODO: fix later`. If it ships, it works. |
| **Clean Go** | `gofmt`, `golangci-lint`, idiomatic error returns, no `panic` in library code. |
| **SQL-standard bias** | When the standard and a vendor dialect conflict, implement the standard first and alias the dialect. |
| **Forward-compatible schema** | Design for SQL:2023 from day one. Never paint yourself into a corner for the sake of an early milestone. |

---

## 2. Agent Roles

### 2.1 Orchestrator Agent
- Reads `NOW.md` to understand current state.
- Selects the next runnable task(s) from `TASKS.md` (respects `depends_on`).
- Spawns Subagents with a bounded scope (single task or tightly coupled task group).
- Writes back to `NOW.md` after each session.
- Never writes implementation code directly.

### 2.2 Subagent — Implementation
- Receives: task ID, acceptance criteria, relevant SPEC sections.
- Produces: Go source, tests, and updated `TASKS.md` checkbox.
- Must not modify `SPEC.md`, `AGENTS.md`, or `INDEX.md` unless the task explicitly says so.
- Commits must be atomic and green (all tests pass).

### 2.3 Subagent — Testing / Eval
- Runs regression suite, SQL compliance tests, and benchmarks.
- Reports failures back to Orchestrator.
- Updates test coverage metrics in `TASKS.md` task notes.

### 2.4 Subagent — Documentation
- Triggered after any public API surface changes.
- Updates `docs/` and in-code GoDoc comments.
- Does not touch implementation files.

### 2.5 Subagent — Review
- Reads a diff and validates against SOLID, KISS, DRY, and SQL-standard bias.
- Outputs a structured review; does not self-merge.

---

## 3. Communication Protocol

```
NOW.md          ← single source of "what is happening right now"
TASKS.md        ← authoritative task list and dependency graph
SPEC.md         ← authoritative behaviour specification
INDEX.md        ← map of the repo so any agent can orient quickly
```

**Every agent session MUST:**
1. Read `NOW.md` first.
2. Read the relevant `SPEC.md` section(s) for its task.
3. On completion, update `NOW.md` (move "next" → "just done", write new "next").
4. Mark the task checkbox in `TASKS.md`.
5. If a decision deviates from `SPEC.md`, open a spec-change note at the bottom of `SPEC.md` and flag it in `NOW.md`.

---

## 4. Parallelism Rules

Tasks tagged `[parallel-safe]` in `TASKS.md` may run simultaneously in separate subagents.
Tasks tagged `[serial]` must complete before any dependent task begins.
The Orchestrator is responsible for detecting and preventing conflicts on shared files.

**Shared files that require serial access:**
- `internal/parser/` (grammar is a monolith until Phase 3)
- `internal/catalog/` (schema registry)
- `go.mod` / `go.sum`

---

## 5. Branch & Commit Convention

```
feat/<milestone>/<task-id>-short-description
fix/<task-id>-short-description
test/<task-id>-short-description
docs/<task-id>-short-description
```

Commit message format:
```
[T-042] Add window function RANK() — parser + executor

- Implements SPEC §8.3.2
- Adds 47 SQL compliance tests
- Passes existing regression suite
```

---

## 6. Definition of Done (per task)

- [ ] All acceptance criteria in `TASKS.md` met
- [ ] Unit tests written (table-driven, Go idiomatic)
- [ ] Integration/SQL compliance tests added where applicable
- [ ] `golangci-lint` passes with zero warnings
- [ ] Public symbols have GoDoc comments
- [ ] `NOW.md` updated
- [ ] `TASKS.md` checkbox ticked

---

## 7. Escalation

If a subagent encounters an ambiguity not resolved by `SPEC.md`:
1. Do not guess. Halt.
2. Write a `SPEC_QUESTION: <task-id>` block at the bottom of `SPEC.md`.
3. Update `NOW.md` with `BLOCKED: waiting on spec clarification`.
4. The Orchestrator or human resolves it before resuming.
