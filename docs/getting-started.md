# Getting Started

## Command-line entry point

`cmd/tucotuco` is the Phase 1 CLI. It can either start an interactive REPL
or execute a SQL script from `--file`.

By default it opens `tucotuco.catalog.json` in the current working directory.
The CLI should emit deterministic, machine-friendly output rather than aligned
tables; JSON or an equivalent stable format is the preferred shape for result
sets.

Run the REPL:

```bash
go run ./cmd/tucotuco
```

Run a script:

```bash
go run ./cmd/tucotuco --file path/to/script.sql
```

Script execution uses a top-level semicolon splitter, so semicolons inside
string literals and comments stay attached to the statement they belong to.

The CLI uses the current Phase 1 engine directly. It does not reimplement SQL
parsing or execution in the command layer.

## Phase 1 storage model

Phase 1 persists catalog metadata only. Opening a database path records schema
and table definitions on disk, but table rows remain in memory until the Phase
2 storage work lands.

That means the CLI is useful for creating schemas, tables, and running
temporary data-changing statements during a session, but it does not yet give
you durable row storage.
