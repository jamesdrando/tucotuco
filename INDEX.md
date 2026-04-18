# INDEX.md — tucotuco Repository Map

> Quick-orientation guide. Every agent reads this before touching code.
> Keep it current whenever directory structure changes (Documentation Subagent owns this file).

---

## Repository Layout

```
tucotuco/
├── AGENTS.md           # Agent collaboration contract (you are here's sibling)
├── INDEX.md            # This file
├── SPEC.md             # Authoritative SQL behaviour specification
├── TASKS.md            # Full task list, milestones, dependency graph
├── NOW.md              # Rolling "what just happened / what is next" log
│
├── cmd/
│   └── tucotuco/       # CLI entry point (REPL + file execution)
│
├── internal/
│   ├── token/          # Lexer tokens and keyword table
│   ├── lexer/          # SQL lexer (streaming, position-aware)
│   ├── ast/            # AST node types (generated + hand-written)
│   ├── parser/         # Recursive-descent SQL parser
│   ├── analyzer/       # Semantic analysis, name resolution, type checking
│   ├── catalog/        # Schema registry (databases, tables, columns, types)
│   ├── types/          # SQL type system (SPEC §3)
│   ├── planner/        # Logical query plan builder
│   ├── optimizer/      # Rule-based + cost-based query optimizer
│   ├── executor/       # Volcano-model physical execution engine
│   ├── storage/        # Storage engine interface + in-memory + on-disk impls
│   │   ├── memory/     # In-memory row store (Phase 1 default)
│   │   └── paged/      # Page-based on-disk store (Phase 2+)
│   ├── index/          # Index manager (B-tree, hash)
│   ├── txn/            # Transaction manager (MVCC)
│   ├── wal/            # Write-ahead log
│   ├── replication/    # (Phase 5+) Replication primitives
│   └── mcp/            # (Phase 7) MCP server integration
│
├── pkg/
│   ├── driver/         # database/sql driver (Go standard interface)
│   ├── wire/           # (Phase 4) PostgreSQL wire protocol compatibility
│   └── embed/          # Embeddable Go API (no CLI dependency)
│
├── compliance/
│   ├── sql92/          # SQL-92 compliance test suite
│   ├── sql99/          # SQL:1999 compliance test suite
│   ├── sql2003/        # SQL:2003 compliance test suite
│   ├── sql2011/        # SQL:2011 compliance test suite
│   ├── sql2016/        # SQL:2016 compliance test suite
│   └── sql2023/        # SQL:2023 compliance test suite
│
├── testdata/
│   ├── queries/        # Named .sql files used in golden tests
│   └── results/        # Expected output files for golden tests
│
├── bench/              # Benchmarks (go test -bench)
│
├── docs/
│   ├── architecture.md # Deep-dive architecture narrative
│   ├── sql-support.md  # Running matrix of supported SQL features
│   ├── storage.md      # Storage engine internals
│   ├── getting-started.md
│   └── api.md          # Go embedding API reference
│
└── scripts/
    ├── gen-ast.go      # AST node code generator
    ├── compliance.sh   # Run full compliance suite and report
    └── bench-compare.sh
```

---

## Key Interfaces (always check these before implementing anything new)

| Interface | Location | Purpose |
|-----------|----------|---------|
| `Storage` | `internal/storage/storage.go` | Row read/write abstraction |
| `Index` | `internal/index/index.go` | Index scan/insert/delete |
| `Catalog` | `internal/catalog/catalog.go` | Schema introspection |
| `Node` (AST) | `internal/ast/node.go` | AST visitor root |
| `Plan` | `internal/planner/plan.go` | Logical plan node |
| `Executor` | `internal/executor/executor.go` | Physical operator interface |
| `Transaction` | `internal/txn/txn.go` | ACID transaction handle |

---

## Spec Cross-Reference

| Topic | SPEC.md Section |
|-------|----------------|
| Type system | §3 |
| Lexer / tokens | §4 |
| Parser grammar | §5 |
| DDL semantics | §6 |
| DML semantics | §7 |
| Query expressions | §8 |
| Window functions | §8.3 |
| CTEs | §8.4 |
| Transactions | §9 |
| Indexes | §10 |
| Constraints | §11 |
| Stored routines | §12 |
| Triggers | §13 |
| Views | §14 |
| Full-text search | §15 |
| JSON support | §16 |
| Temporal tables | §17 |
| Row-pattern recognition | §18 |
| MCP integration | §20 |
| Skills layer | §21 |
| External integrations | §22 |

---

## Compliance Test Runner

```bash
# Run all compliance tests for a specific standard
go test ./compliance/sql92/...

# Run full suite and generate coverage matrix
./scripts/compliance.sh

# Benchmark comparison against baseline
./scripts/bench-compare.sh main HEAD
```

---

## Lint & Format

```bash
gofmt -w ./...
golangci-lint run ./...
```

All CI gates require lint-clean, test-green, and zero race detector hits.
