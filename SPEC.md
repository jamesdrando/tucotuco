# SPEC.md — tucotuco SQL Engine Specification

> **Authoritative source of truth** for all behavioural decisions.
> When implementation and spec conflict, fix the implementation.
> When the spec is silent, open a `SPEC_QUESTION` block at the bottom of this file.

---

## §1 Project Goals

tucotuco is a **pure-Go**, **embeddable** SQL engine with the following target feature set (in priority order):

1. Full **SQL:2023** core compliance
2. `database/sql` driver compatibility
3. **PostgreSQL wire protocol** (read/query path first)
4. **MCP (Model Context Protocol)** server — expose SQL as an AI tool
5. **Skills layer** — user-defined AI-augmented functions
6. **Integrations** — CSV, Parquet, JSON, REST, S3, DuckDB interchange

**Non-goals (v1.0):** distributed execution, query federation across heterogeneous stores, full DDL replication.

---

## §2 Versioning & Compatibility Policy

- Public Go API follows **semver**. No breaking changes without a major version bump.
- SQL dialect: when SQL:2023 conflicts with an older standard, SQL:2023 wins.
- Vendor extensions (MySQL, PostgreSQL dialect syntax) may be added as **opt-in parser modes** but must never be the default.
- Deprecated SQL features are parsed and executed but emit a `WARNING` to the diagnostics stream.

---

## §3 Type System

### §3.1 Exact Numerics
| SQL Type | Go representation | Notes |
|----------|------------------|-------|
| `SMALLINT` | `int16` | |
| `INTEGER` / `INT` | `int32` | |
| `BIGINT` | `int64` | |
| `NUMERIC(p,s)` / `DECIMAL(p,s)` | `*apd.Decimal` | cockroachdb/apd |
| `BOOLEAN` | `bool` | |

### §3.2 Approximate Numerics
| SQL Type | Go representation |
|----------|------------------|
| `REAL` | `float32` |
| `DOUBLE PRECISION` / `FLOAT` | `float64` |

### §3.3 Character Strings
| SQL Type | Go representation | Notes |
|----------|------------------|-------|
| `CHAR(n)` | `string` | right-padded to n on storage |
| `VARCHAR(n)` | `string` | max n bytes |
| `TEXT` (extension) | `string` | unbounded |
| `CLOB` | `string` | large object, may be lazy-loaded |

Collation: UTF-8 by default. `COLLATE` clause selects from registered collators.

### §3.4 Binary Strings
| SQL Type | Go representation |
|----------|------------------|
| `BINARY(n)` | `[]byte` |
| `VARBINARY(n)` | `[]byte` |
| `BLOB` | `[]byte` / lazy |

### §3.5 Date/Time
| SQL Type | Go representation | Notes |
|----------|------------------|-------|
| `DATE` | `time.Time` (date-only) | Store as days since epoch |
| `TIME` | `time.Duration` | |
| `TIME WITH TIME ZONE` | `time.Time` | Preserve offset |
| `TIMESTAMP` | `time.Time` | UTC internally |
| `TIMESTAMP WITH TIME ZONE` | `time.Time` | Preserve offset |
| `INTERVAL` | `internal/types.Interval` | Year-month + day-time components |

### §3.6 JSON (SQL:2016+)
- `JSON` type stored as `[]byte` (validated UTF-8 JSON).
- Path expressions: SQL/JSON path language (`$.key`, `$[0]`, etc.).
- Functions: `JSON_VALUE`, `JSON_QUERY`, `JSON_TABLE`, `JSON_EXISTS`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_ARRAYAGG`, `JSON_OBJECTAGG`.

### §3.7 Array Types (SQL:2003+)
- `<type> ARRAY[n]` or `<type> ARRAY`.
- Go representation: `[]Value`.
- Subscript access: `arr[1]` (1-based, SQL standard).

### §3.8 Row Types
- `ROW(f1 t1, f2 t2, ...)` — anonymous row constructor.
- Named row types via `CREATE TYPE`.

### §3.9 User-Defined Types (SQL:2003+)
- Distinct types: `CREATE TYPE my_int AS INTEGER`.
- Structured types: `CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50))`.

### §3.10 NULL Semantics
- NULL is a first-class value in all types.
- Three-valued logic: TRUE, FALSE, UNKNOWN.
- `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`.
- Aggregate functions ignore NULLs unless specified.

---

## §4 Lexer

- Streaming tokeniser; position-aware (line + column + byte offset).
- Case-insensitive keywords; identifiers are case-sensitive unless quoted.
- Quoted identifiers: `"ident"` (standard) and `` `ident` `` (extension, opt-in).
- String literals: `'text'`. Escape: `''` for embedded single quote. `E'...'` extension for C-style escapes (opt-in).
- Numeric literals: integer, decimal, scientific notation, hex (`0x`).
- Comments: `-- line` and `/* block */`.
- Unicode escapes in identifiers: `U&"..." UESCAPE '!'`.

---

## §5 Parser Grammar

- Recursive-descent, hand-written (no parser generator dependency).
- Error recovery: synchronise on statement-level semicolons.
- Every parse error includes line, column, and a human-readable message.
- Produces a concrete syntax tree first; a separate AST-lowering pass strips trivia.
- Grammar source of truth: `internal/parser/grammar.ebnf` (checked in, but not executed — documentation only).

### §5.1 Supported Statement Types (milestone-gated; see TASKS.md)

```
DDL:  CREATE TABLE, ALTER TABLE, DROP TABLE
      CREATE INDEX, DROP INDEX
      CREATE VIEW, DROP VIEW
      CREATE TYPE, DROP TYPE
      CREATE SCHEMA, DROP SCHEMA
      CREATE SEQUENCE, DROP SEQUENCE
      CREATE TRIGGER, DROP TRIGGER
      CREATE PROCEDURE, DROP PROCEDURE
      CREATE FUNCTION, DROP FUNCTION

DML:  SELECT (full — see §8)
      INSERT, INSERT ... SELECT, INSERT ... ON CONFLICT
      UPDATE, UPDATE ... FROM
      DELETE, TRUNCATE
      MERGE (SQL:2003+)

TCL:  BEGIN, COMMIT, ROLLBACK
      SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT
      SET TRANSACTION

Other: CALL, EXECUTE
       EXPLAIN, EXPLAIN ANALYZE
       SET, SHOW
       COPY (extension)
```

---

## §6 DDL Semantics

### §6.1 CREATE TABLE
- Column constraints: `NOT NULL`, `DEFAULT`, `CHECK`, `UNIQUE`, `PRIMARY KEY`, `REFERENCES`.
- Table constraints: `CONSTRAINT name ...` versions of all column constraints + multi-column variants.
- `GENERATED ALWAYS AS (expr) STORED` (SQL:2003 computed columns).
- `GENERATED ALWAYS AS IDENTITY` / `GENERATED BY DEFAULT AS IDENTITY` (SQL:2003 identity columns).
- Temporal: `PERIOD FOR SYSTEM_TIME` / `PERIOD FOR APPLICATION_TIME` (SQL:2011).

### §6.2 ALTER TABLE
- `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN`.
- `ADD CONSTRAINT`, `DROP CONSTRAINT`.
- `RENAME COLUMN`, `RENAME TABLE`.

### §6.3 Indexes
- B-tree default. Hash index opt-in.
- Composite, partial (`WHERE` clause), expression indexes.
- `UNIQUE` indexes enforced at write time.

---

## §7 DML Semantics

### §7.1 INSERT
- `VALUES`, `SELECT`, `DEFAULT VALUES`.
- `ON CONFLICT DO NOTHING` / `ON CONFLICT DO UPDATE` (SQL:2003 MERGE subset).
- Returning clause: `RETURNING *` / `RETURNING expr-list`.

### §7.2 UPDATE
- Standard `SET col = expr` and `SET (col1, col2) = (expr1, expr2)`.
- `FROM` join extension (mirroring SQL:2003 and PostgreSQL).
- `RETURNING`.

### §7.3 DELETE
- Standard `WHERE` clause.
- `RETURNING`.

### §7.4 MERGE (SQL:2003+)
- `MERGE INTO target USING source ON condition WHEN MATCHED ... WHEN NOT MATCHED ...`

---

## §8 Query Expressions (SELECT)

### §8.1 Clauses (in evaluation order)
1. `FROM` + `JOIN` (cross, inner, left/right/full outer, natural, lateral)
2. `WHERE`
3. `GROUP BY` (including `ROLLUP`, `CUBE`, `GROUPING SETS` — SQL:1999)
4. `HAVING`
5. Window function computation
6. `SELECT` list (projection + aliasing)
7. `DISTINCT` / `DISTINCT ON` (extension)
8. `ORDER BY` (including `NULLS FIRST` / `NULLS LAST`)
9. `OFFSET` / `FETCH FIRST n ROWS ONLY` (SQL:2008) — alias `LIMIT` as extension
10. `FOR UPDATE` / `FOR SHARE`

### §8.2 Subqueries
- Scalar, correlated, lateral.
- `EXISTS`, `NOT EXISTS`.
- `IN`, `NOT IN`, `ANY`, `ALL`, `SOME`.
- Derived tables in `FROM`.

### §8.3 Window Functions (SQL:2003+)
- `OVER (PARTITION BY ... ORDER BY ... frame)`.
- Frame modes: `ROWS`, `RANGE`, `GROUPS` (SQL:2011).
- Frame bounds: `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, `UNBOUNDED FOLLOWING`.
- `EXCLUDE CURRENT ROW`, `EXCLUDE GROUP`, `EXCLUDE TIES`, `EXCLUDE NO OTHERS`.
- Built-in window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`.
- All aggregate functions may be used as window functions.

### §8.4 Common Table Expressions (SQL:1999+)
- `WITH cte AS (...)`.
- Recursive: `WITH RECURSIVE`.
- `SEARCH BREADTH FIRST` / `SEARCH DEPTH FIRST` (SQL:1999).
- `CYCLE` detection (SQL:1999).
- Materialization hint: `WITH ... AS MATERIALIZED` / `AS NOT MATERIALIZED` (SQL:2011).

### §8.5 Set Operations
- `UNION`, `INTERSECT`, `EXCEPT` — ALL and DISTINCT variants.
- Nesting and precedence per standard (INTERSECT before UNION/EXCEPT).

### §8.6 Scalar Functions — Built-in Library

**String:** `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTRING`, `POSITION`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `OCTET_LENGTH`, `OVERLAY`, `CONCAT`, `LIKE`, `SIMILAR TO`, `REGEXP_LIKE` (SQL:2008), `REGEXP_REPLACE`, `REGEXP_SUBSTR`.

**Numeric:** `ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNCATE`, `MOD`, `POWER`, `SQRT`, `EXP`, `LN`, `LOG`, `LOG10`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SIGN`, `GREATEST`, `LEAST`, `RANDOM` (extension).

**Date/Time:** `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `EXTRACT`, `DATE_TRUNC` (extension), `DATE_ADD` (extension), `AGE` (extension), `TO_CHAR` (extension).

**Conditional:** `CASE WHEN`, `COALESCE`, `NULLIF`, `IIF` (extension).

**Type conversion:** `CAST`, `TRY_CAST` (extension).

**Aggregate:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `EVERY`, `ANY_VALUE`, `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`, `STRING_AGG`, `ARRAY_AGG`, `JSON_ARRAYAGG`, `JSON_OBJECTAGG`.

---

## §9 Transaction Model

- **Isolation levels:** READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE.
- Default: READ COMMITTED.
- Implementation: MVCC with optimistic concurrency for higher isolation levels.
- `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` must be fully functional before any persistence layer ships.
- DDL is transactional (like PostgreSQL, unlike MySQL).

---

## §10 Index Access

- Planner selects index scans over full table scans when selectivity justifies it.
- Statistics: per-column row count estimates refreshed on `ANALYZE`.
- Multi-column indexes: leftmost-prefix rule for equality; full composite for range.
- Covering indexes: index-only scans when all projected columns are in index.

---

## §11 Constraints

- `NOT NULL`: enforced at write time, propagated through type checker.
- `CHECK`: expression evaluated at write time; `FALSE` or `UNKNOWN` rejects the row.
- `UNIQUE` / `PRIMARY KEY`: maintained via unique B-tree index.
- `FOREIGN KEY`: `ON DELETE` / `ON UPDATE` actions: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION`.
- Deferred constraints: `DEFERRABLE INITIALLY DEFERRED` / `DEFERRABLE INITIALLY IMMEDIATE`.

---

## §12 Stored Routines (SQL:2003+)

- `CREATE FUNCTION` — deterministic and non-deterministic.
- `CREATE PROCEDURE` — called via `CALL`.
- Language: SQL PSM (Persistent Stored Modules) first; external Go UDF second.
- Control flow: `IF/THEN/ELSE`, `CASE`, `LOOP`, `WHILE`, `REPEAT`, `FOR`, `ITERATE`, `LEAVE`.
- Cursors, condition handlers, local variables.

---

## §13 Triggers (SQL:2003+)

- `BEFORE` / `AFTER` / `INSTEAD OF` (for views).
- `INSERT`, `UPDATE`, `DELETE` events.
- `FOR EACH ROW` / `FOR EACH STATEMENT`.
- `WHEN` clause.
- Transition tables: `REFERENCING OLD TABLE AS ...` / `NEW TABLE AS ...` (SQL:1999).

---

## §14 Views

- `CREATE VIEW` — logical view, always computed at query time.
- `WITH CHECK OPTION`.
- `CREATE MATERIALIZED VIEW` (extension, Phase 4+).
- Updatable views: single-table, no `DISTINCT`/`GROUP BY`/aggregates.

---

## §15 Full-Text Search (extension, Phase 5+)

- `tsvector` / `tsquery` types (PostgreSQL-compatible extension).
- GIN index support for full-text.
- Functions: `to_tsvector`, `to_tsquery`, `ts_rank`.

---

## §16 JSON (SQL:2016+)

See §3.6 for type details.

- `JSON_TABLE` produces a relational result set from a JSON document.
- Path operators: `->` and `->>` (extension aliases for `JSON_QUERY` / `JSON_VALUE`).
- `IS JSON` predicate.
- `JSON_EXISTS` predicate.

---

## §17 Temporal Tables (SQL:2011+)

- **System-time temporals:** `PERIOD FOR SYSTEM_TIME (start, end)` on `CREATE TABLE`. Automatically maintained by the engine.
- **Application-time temporals:** user-managed period columns.
- Temporal predicates: `AS OF SYSTEM TIME`, `FROM ... TO ...`, `BETWEEN ... AND ...`, `CONTAINED IN`.
- `FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP` in `FROM` clause.

---

## §18 Row Pattern Recognition (SQL:2016+)

- `MATCH_RECOGNIZE` clause.
- Pattern variables, quantifiers, `DEFINE`, `MEASURES`, `ONE ROW PER MATCH` / `ALL ROWS PER MATCH`.

---

## §19 Error Model

- Every error has: `SQLSTATE` (5-char standard code), human message, source location.
- Warnings are returned alongside results, not instead of them.
- `SIGNAL` / `RESIGNAL` within stored routines.

---

## §20 MCP Integration (Phase 7)

- tucotuco exposes itself as an MCP server.
- Tools exposed: `sql_query`, `sql_execute`, `schema_inspect`, `explain_query`.
- Authentication: API key or OAuth2 (pluggable).
- Transport: stdio (local) and HTTP/SSE (remote).
- Schema is discoverable via MCP resource listing.

---

## §21 Skills Layer (Phase 8)

- `CREATE SKILL name LANGUAGE GO AS '...'` — registers a Go function callable from SQL.
- `CREATE SKILL name LANGUAGE LLM USING model '...' PROMPT '...'` — LLM-backed scalar function.
- Skills are sandboxed: no direct DB access except via SQL calls.
- Skills may be `DETERMINISTIC` (cached) or `VOLATILE`.

---

## §22 External Integrations (Phase 8+)

- **CSV:** `CREATE FOREIGN TABLE t (...) SERVER csv_server OPTIONS (file '...')`.
- **Parquet:** Same pattern, `parquet_server`.
- **JSON Lines:** `jsonl_server`.
- **REST:** `rest_server` with URL template and JSON path mappings.
- **S3/GCS/Azure Blob:** Object-store foreign table wrappers.
- **DuckDB interchange:** Arrow IPC / ADBC protocol.

---

## SPEC_QUESTIONS

_No open questions._

> To add a question: append a block:
> ```
> SPEC_QUESTION [task-id] YYYY-MM-DD
> Question text here.
> ```
