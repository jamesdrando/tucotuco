# TASKS.md — tucotuco Full Task List

> **Legend**
> - `[parallel-safe]` — can run concurrently with other parallel-safe tasks in the same phase
> - `[serial]` — must complete before dependents start
> - `depends: T-xxx` — task IDs that must be ✅ before this task starts
> - `milestone:` — the milestone this task counts toward
> - Checkboxes: `[ ]` open · `[x]` complete · `[~]` in progress · `[!]` blocked

---

## Phase 0 — Repo Bootstrap `[serial]`

> Goal: Empty repo → compiling skeleton with CI.
> All Phase 0 tasks are serial and must finish before any Phase 1 work begins.

- [x] **T-001** — Initialise Go module `github.com/yourorg/tucotuco`, `go 1.22+` `[serial]`
  - Acceptance: `go build ./...` succeeds on empty skeleton
  - Deliver: `go.mod`, `go.sum`, root `doc.go`

- [x] **T-002** — Establish directory layout per `INDEX.md` `[serial]` `depends: T-001`
  - Acceptance: All `internal/` and `pkg/` packages exist as empty stubs with package declarations

- [x] **T-003** — CI pipeline (GitHub Actions or equivalent) `[serial]` `depends: T-001`
  - Jobs: `lint` (golangci-lint), `test` (race detector on), `build`
  - Acceptance: All three jobs green on push

- [x] **T-004** — `golangci-lint` config (`.golangci.yml`) `[parallel-safe]` `depends: T-001`
  - Enable: `errcheck`, `govet`, `staticcheck`, `unused`, `gofmt`, `godot`, `revive`

- [x] **T-005** — `Makefile` with targets: `build`, `test`, `lint`, `bench`, `compliance` `[parallel-safe]` `depends: T-002`

- [x] **T-006** — Logging / diagnostics package `internal/diag` `[serial]` `depends: T-002`
  - Structured `slog`-based; carries `SQLSTATE`, source position
  - Acceptance: unit tests for Error, Warning, Info levels

---
**MILESTONE M0: Repo Ready** — T-001 through T-006 complete.

---

## Phase 1 — Core Engine (In-Memory, SQL-92 Subset) `milestone: M1`

> Goal: `SELECT 1`, `CREATE TABLE`, `INSERT`, `SELECT ... FROM ... WHERE` work end-to-end.
> Many tasks here are parallel-safe once their direct dependency is met.

### 1A — Type System

- [x] **T-010** — Define `Value` union type (SPEC §3) `[serial]` `depends: M0`
  - Represents: NULL, bool, int16/32/64, float32/64, string, []byte, time.Time, Interval, Decimal, Array, Row
  - Acceptance: `Value.Equal`, `Value.Compare`, `Value.IsNull` all tested

- [x] **T-011** — SQL type descriptors (`TypeDesc`) `[parallel-safe]` `depends: T-010`
  - `TypeKind` enum, precision/scale/length, nullable flag
  - Acceptance: round-trip serialise/deserialise

- [x] **T-012** — Type coercion rules (implicit cast table, SPEC §3) `[parallel-safe]` `depends: T-011`
  - Acceptance: 50+ coercion rule tests covering all type pairs

- [x] **T-013** — `CAST` / `TRY_CAST` executor functions `[parallel-safe]` `depends: T-012`

### 1B — Lexer

- [x] **T-020** — Keyword table (SQL-92 reserved + non-reserved words) `[parallel-safe]` `depends: M0`
  - File: `internal/token/keywords.go` (generated from embedded list)

- [x] **T-021** — Lexer core: tokenise SQL byte stream `[serial]` `depends: T-020`
  - Tokens: keyword, identifier, string literal, numeric literal, operator, punctuation, EOF, ERROR
  - Position tracking: line, column, byte offset
  - Acceptance: 200+ lexer unit tests; fuzz target `FuzzLexer`

- [x] **T-022** — Extend keyword table for SQL-99 additions `[parallel-safe]` `depends: T-021`

### 1C — AST

- [x] **T-030** — AST node interface and visitor pattern `[serial]` `depends: M0`
  - `Node` interface: `Pos() token.Pos`, `End() token.Pos`, `Accept(Visitor) any`
  - Visitor interface with one method per node type

- [x] **T-031** — Literal expression nodes (integer, float, string, bool, NULL, param) `[parallel-safe]` `depends: T-030`

- [x] **T-032** — Identifier, qualified name, star nodes `[parallel-safe]` `depends: T-030`

- [x] **T-033** — Binary/unary expression nodes `[parallel-safe]` `depends: T-031,T-032`

- [x] **T-034** — SELECT statement AST (FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT) `[serial]` `depends: T-033`

- [x] **T-035** — INSERT / UPDATE / DELETE AST nodes `[parallel-safe]` `depends: T-033`

- [x] **T-036** — CREATE TABLE / DROP TABLE AST nodes `[parallel-safe]` `depends: T-033`

- [x] **T-037** — AST pretty-printer (for debug and golden tests) `[parallel-safe]` `depends: T-034,T-035,T-036`
  - Implemented in `internal/ast/pretty.go` and `internal/ast/pretty_test.go`; visitor-backed structural printer and table-driven goldens validated with targeted package tests and lint.

### 1D — Parser

- [x] **T-040** — Parser scaffold: token stream, `peek`/`consume`, error recovery `[serial]` `depends: T-021,T-030`

- [x] **T-041** — Parse scalar expressions (literals, identifiers, operators, CASE, CAST, function calls) `[serial]` `depends: T-040,T-031,T-032,T-033`
  - Operator precedence: full SQL-92 precedence table
  - Acceptance: 100+ expression parse tests; fuzz target `FuzzParseExpr`

- [x] **T-042** — Parse SELECT statement (all SQL-92 clauses) `[serial]` `depends: T-041,T-034`
  - Acceptance: 100+ SELECT parse tests; golden AST output
  - Parser-local CST and focused parse tests landed in `internal/parser` for `SELECT` clauses, joins, derived tables, and ordering; validated with targeted package tests and lint.

- [x] **T-043** — Parse INSERT / UPDATE / DELETE `[parallel-safe]` `depends: T-041,T-035`
  - Parser-local CST and focused parse tests landed in `internal/parser` for `INSERT` (`VALUES` / `SELECT` / `DEFAULT VALUES`), `UPDATE`, and `DELETE`; validated with targeted package tests and lint.

- [x] **T-044** — Parse CREATE TABLE / DROP TABLE `[parallel-safe]` `depends: T-041,T-036`
  - Parser-local CST and focused parse tests landed in `internal/parser` for `CREATE TABLE` / `DROP TABLE`, including flat type, constraint, and reference metadata; validated with targeted package tests and lint.

- [x] **T-045** — Parse BEGIN / COMMIT / ROLLBACK `[parallel-safe]` `depends: T-040`

- [x] **T-046** — Multi-statement parsing (semicolon-separated) `[parallel-safe]` `depends: T-042,T-043,T-044,T-045`
  - `ParseScript` now skips empty semicolon-only segments while preserving existing semicolon recovery and EOF behavior; focused parser tests for repeated/trailing semicolons and recovery validated with targeted package tests and lint.

### 1E — Catalog

- [x] **T-050** — `Catalog` interface + in-memory implementation `[serial]` `depends: T-011`
  - Operations: `CreateSchema`, `DropSchema`, `CreateTable`, `DropTable`, `LookupTable`, `LookupColumn`
  - Thread-safe

- [x] **T-051** — Column descriptor, table descriptor, schema descriptor `[parallel-safe]` `depends: T-050`

- [x] **T-052** — Catalog persistence (write catalog to disk as JSON/gob, reload on open) `[serial]` `depends: T-051`
  - Phase 1 uses a simple file; Phase 2 moves to WAL-backed pages
  - Implemented Phase 1 versioned JSON persistence via `SaveFile` / `LoadFile` in `internal/catalog`, replaying `CreateSchema` / `CreateTable` on load with round-trip and isolation tests; validated with targeted package tests and lint.

### 1F — Semantic Analyzer

- [x] **T-060** — Name resolution pass (resolve identifiers against catalog) `[serial]` `depends: T-042,T-050`
  - Implemented in `internal/analyzer` as a catalog-backed name-resolution pass over the parser-local CST, including derived-table and query-local scopes, foreign-key reference resolution, and focused analyzer tests; validated with targeted package tests and lint.

- [x] **T-061** — Type checker pass (assign types to all expression nodes) `[serial]` `depends: T-060,T-012`
  - Implemented a side-table-based analyzer type checker in `internal/analyzer` that assigns types to the current Phase 1 CST expression/query surface, normalizes parser type names for `CAST`/`CREATE TABLE`, enforces boolean/assignment/default contexts, and validates with focused analyzer tests plus targeted package lint/test runs.

- [x] **T-062** — Constraint validation (NOT NULL, column count in INSERT) `[parallel-safe]` `depends: T-061`
  - Added analyzer-side write validation for `INSERT`/`UPDATE` shape, omitted required columns, `DEFAULT VALUES`, explicit `NULL` writes to `NOT NULL` targets, and `NOT NULL DEFAULT NULL` in `CREATE TABLE`; validated with focused analyzer package tests.

- [x] **T-063** — Aggregate / window function placement validator `[parallel-safe]` `depends: T-061`
  - Added aggregate placement and grouped-query validation over the current Phase 1 CST surface in `internal/analyzer`, including disallowed-clause diagnostics, nested aggregate rejection, derived-table recursion, and focused analyzer package tests; current parser surface still has no `OVER` syntax to validate.

### 1G — Storage Engine

- [x] **T-070** — `Storage` interface: `Insert`, `Scan`, `Update`, `Delete`, `NewTransaction` `[serial]` `depends: M0`

- [x] **T-071** — In-memory row store (`internal/storage/memory`) `[serial]` `depends: T-070,T-010`
  - Heap: slice of `[]Value` rows
  - Concurrent-read, exclusive-write via RWMutex
  - Acceptance: 50+ storage unit tests

- [x] **T-072** — Sequential scan operator `[parallel-safe]` `depends: T-071`

- [x] **T-073** — In-memory transaction isolation (snapshot per-transaction) `[parallel-safe]` `depends: T-071`

### 1H — Query Planner (Logical)

- [x] **T-080** — Logical plan node interface and basic nodes: `Scan`, `Filter`, `Project`, `Limit` `[serial]` `depends: T-062`

- [x] **T-081** — Plan builder: translate analyzed AST → logical plan `[serial]` `depends: T-080`

- [x] **T-082** — Logical plan printer (EXPLAIN output) `[parallel-safe]` `depends: T-081`

### 1I — Executor (Volcano Model)

- [x] **T-090** — `Operator` interface: `Open()`, `Next() (Row, error)`, `Close()` `[serial]` `depends: T-080`

- [x] **T-091** — SeqScan operator `[parallel-safe]` `depends: T-090,T-072`

- [x] **T-092** — Filter operator `[parallel-safe]` `depends: T-090`

- [x] **T-093** — Projection operator `[parallel-safe]` `depends: T-090`

- [x] **T-094** — Expression evaluator (`eval(expr, row) Value`) `[serial]` `depends: T-090,T-013`
  - Covers all SPEC §8.6 scalar functions for SQL-92 subset

- [x] **T-095** — Sort operator `[parallel-safe]` `depends: T-090,T-094`

- [x] **T-096** — Limit/Offset operator `[parallel-safe]` `depends: T-090`

- [x] **T-097** — HashAggregate operator (GROUP BY + aggregate functions) `[serial]` `depends: T-094`

- [x] **T-098** — INSERT / UPDATE / DELETE executors `[parallel-safe]` `depends: T-094,T-071`

- [x] **T-099** — DDL executor: CREATE TABLE, DROP TABLE `[parallel-safe]` `depends: T-051,T-071`

### 1J — Public API

- [x] **T-100** — `pkg/embed` API: `Open(path) (*DB, error)`, `DB.Exec`, `DB.Query`, `DB.Begin` `[serial]` `depends: T-098,T-099,T-082`

- [x] **T-101** — `pkg/driver`: `database/sql` driver registration `[serial]` `depends: T-100`

- [x] **T-102** — `cmd/tucotuco`: REPL + `--file` flag for script execution `[parallel-safe]` `depends: T-100`

### 1K — Testing Infrastructure

- [x] **T-110** — Golden-test harness: `.sql` input → `.txt` expected output `[serial]` `depends: T-102`

- [x] **T-111** — SQL-92 compliance suite skeleton (50 minimum tests) `[parallel-safe]` `depends: T-110`

- [x] **T-112** — Race-detector CI job `[parallel-safe]` `depends: T-003`

- [x] **T-113** — Fuzz targets: lexer, parser, expression evaluator `[parallel-safe]` `depends: T-021,T-046,T-094`


---

## Phase 2 — SQL-92 Full Compliance + Persistent Storage `milestone: M2`

> Parallelisable streams: Storage (2A), SQL features (2B), Constraints (2C), Indexes (2D).

### 2A — Persistent Storage

- [x] **T-120** — Page layout design: slotted pages, page header, tuple format `[serial]` `depends: M1`
  - Document: `docs/storage.md`

- [x] **T-121** — Buffer pool manager (LRU eviction, dirty page tracking) `[serial]` `depends: T-120`

- [x] **T-122** — Heap file manager (table → set of pages) `[serial]` `depends: T-121`

- [x] **T-123** — WAL (write-ahead log): log record format, append, fsync `[serial]` `depends: T-121`

- [x] **T-124** — WAL recovery: redo pass on restart `[serial]` `depends: T-123`

- [x] **T-125** — MVCC row versioning (xmin/xmax per tuple) `[serial]` `depends: T-122,T-124`

- [x] **T-126** — Transaction manager: begin/commit/rollback with MVCC `[serial]` `depends: T-125`

- [x] **T-127** — Vacuum / dead tuple reclamation `[parallel-safe]` `depends: T-126`

- [x] **T-128** — Migrate in-memory storage tests to paged storage `[serial]` `depends: T-126`
  - Shared storage behavior coverage now lives in `internal/storage/storagetest`.
  - `internal/storage/memory/store_test.go` and `internal/storage/paged/storage_contract_test.go` both execute the shared suite, while backend-specific handle semantics remain covered in backend-specific tests.

### 2B — SQL-92 Feature Completeness

- [x] **T-130** — JOIN: INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER, CROSS `[serial]` `depends: M1`
  - Nested-loop join executor first; hash join in Phase 3
  - Added a logical planner join node plus nested-loop executor/lowering support for `INNER`, `LEFT`, `RIGHT`, `FULL`, and comma/CROSS joins, preserving joined-column source metadata and outer-join nullability through planner/executor/embed/compliance coverage.
  - `JOIN ... USING` and `NATURAL JOIN` remain explicit feature errors in this baton; their merged-column semantics are still open beyond `T-130`.

- [x] **T-131** — Subqueries: scalar, `EXISTS`, `IN`, correlated `[serial]` `depends: M1`
  - Added explicit parser/analyzer support for scalar subqueries, `EXISTS`, `IN (SELECT ...)`, and correlated outer-scope resolution with local-first shadowing while keeping derived-table `FROM` subqueries non-lateral.
  - Extended embed/executor lowering to execute scalar/`EXISTS`/`IN` subqueries with correlated outer-row binding, scalar cardinality diagnostics, planner `Join` lowering for comma/CROSS paths, and new SQL-92 golden coverage in `061_064`.

- [ ] **T-132** — UNION / INTERSECT / EXCEPT `[parallel-safe]` `depends: M1`

- [ ] **T-133** — CASE expression (searched and simple) `[parallel-safe]` `depends: M1`

- [ ] **T-134** — LIKE / NOT LIKE pattern matching `[parallel-safe]` `depends: M1`

- [ ] **T-135** — Full NULL semantics: IS NULL, IS NOT NULL, three-valued logic everywhere `[parallel-safe]` `depends: M1`

- [ ] **T-136** — Full SQL-92 scalar function library (SPEC §8.6 string + numeric) `[parallel-safe]` `depends: M1`

- [ ] **T-137** — Full SQL-92 aggregate functions `[parallel-safe]` `depends: M1`

- [ ] **T-138** — CREATE SCHEMA / DROP SCHEMA `[parallel-safe]` `depends: T-099`

- [ ] **T-139** — Qualified names (`schema.table.column`) `[parallel-safe]` `depends: T-060`

- [ ] **T-140** — CREATE VIEW / DROP VIEW (logical) `[serial]` `depends: T-081`

- [ ] **T-141** — EXPLAIN (logical plan text output) `[parallel-safe]` `depends: T-082`

### 2C — Constraints

- [ ] **T-150** — NOT NULL enforcement at write time `[parallel-safe]` `depends: T-098`

- [ ] **T-151** — CHECK constraint expression evaluation `[parallel-safe]` `depends: T-094`

- [ ] **T-152** — PRIMARY KEY constraint + unique enforcement `[serial]` `depends: T-126`

- [ ] **T-153** — FOREIGN KEY constraint (RESTRICT + NO ACTION) `[serial]` `depends: T-152`

- [ ] **T-154** — FOREIGN KEY CASCADE / SET NULL / SET DEFAULT `[parallel-safe]` `depends: T-153`

- [ ] **T-155** — UNIQUE constraint (separate from PK) `[parallel-safe]` `depends: T-152`

### 2D — Indexes

- [ ] **T-160** — B-tree index structure (in-memory first) `[serial]` `depends: M1`

- [ ] **T-161** — Index scan operator `[serial]` `depends: T-160,T-090`

- [ ] **T-162** — Unique index enforcement `[parallel-safe]` `depends: T-161`

- [ ] **T-163** — CREATE INDEX / DROP INDEX DDL `[parallel-safe]` `depends: T-161,T-044`

- [ ] **T-164** — Planner rule: prefer index scan over seq scan for point lookups `[serial]` `depends: T-163,T-081`

- [ ] **T-165** — Persistent B-tree (paged, WAL-logged) `[serial]` `depends: T-126,T-160`

### 2E — Compliance Testing

- [ ] **T-170** — SQL-92 compliance suite: expand to 500+ tests `[parallel-safe]` `depends: T-111`

- [ ] **T-171** — Regression test gate: no new failures allowed (CI enforced) `[serial]` `depends: T-170`

---
**MILESTONE M2: SQL-92 Complete** — Full SQL-92 with persistent storage, MVCC, indexes, and constraints. All 500+ SQL-92 compliance tests pass.

---

## Phase 3 — SQL:1999 (SQL-99) `milestone: M3`

> Parallelisable: OO types (3A), CTEs (3B), OLAP (3C), optimizer (3D).

### 3A — SQL:1999 Type Extensions

- [ ] **T-200** — ARRAY type: storage, constructor, subscript access `[serial]` `depends: M2`
- [ ] **T-201** — ROW type: constructor, field access `[parallel-safe]` `depends: T-200`
- [ ] **T-202** — Distinct user-defined types: CREATE TYPE AS `[parallel-safe]` `depends: T-200`
- [ ] **T-203** — BOOLEAN type (if not already done in M1) `[parallel-safe]` `depends: M2`
- [ ] **T-204** — LARGE OBJECT types: CLOB, BLOB (lazy loading) `[parallel-safe]` `depends: M2`

### 3B — CTEs and Recursion

- [ ] **T-210** — WITH (non-recursive CTE) `[serial]` `depends: M2`
- [ ] **T-211** — WITH RECURSIVE (recursive CTE with cycle guard) `[serial]` `depends: T-210`
- [ ] **T-212** — SEARCH BREADTH/DEPTH FIRST in recursive CTE `[parallel-safe]` `depends: T-211`
- [ ] **T-213** — CYCLE clause in recursive CTE `[parallel-safe]` `depends: T-211`

### 3C — OLAP / Grouping

- [ ] **T-220** — ROLLUP grouping set `[serial]` `depends: M2`
- [ ] **T-221** — CUBE grouping set `[parallel-safe]` `depends: T-220`
- [ ] **T-222** — GROUPING SETS `[parallel-safe]` `depends: T-220`
- [ ] **T-223** — GROUPING() function `[parallel-safe]` `depends: T-222`

### 3D — Optimizer

- [ ] **T-230** — Statistics collection (`ANALYZE` command) `[serial]` `depends: M2`
- [ ] **T-231** — Selectivity estimation for predicates `[serial]` `depends: T-230`
- [ ] **T-232** — Cost model: seq scan vs index scan cost `[serial]` `depends: T-231`
- [ ] **T-233** — Hash join executor `[parallel-safe]` `depends: M2`
- [ ] **T-234** — Merge join executor `[parallel-safe]` `depends: M2`
- [ ] **T-235** — Planner join order enumeration (DP for ≤8 tables) `[serial]` `depends: T-232,T-233,T-234`
- [ ] **T-236** — Predicate pushdown rule `[parallel-safe]` `depends: T-232`
- [ ] **T-237** — Projection pruning rule `[parallel-safe]` `depends: T-232`
- [ ] **T-238** — Subquery unnesting (scalar and EXISTS) `[serial]` `depends: T-235`
- [ ] **T-239** — EXPLAIN ANALYZE (runtime stats) `[parallel-safe]` `depends: T-235`

### 3E — Triggers

- [ ] **T-240** — BEFORE / AFTER row triggers `[serial]` `depends: M2`
- [ ] **T-241** — Statement-level triggers `[parallel-safe]` `depends: T-240`
- [ ] **T-242** — INSTEAD OF triggers on views `[parallel-safe]` `depends: T-240,T-140`
- [ ] **T-243** — Transition tables (REFERENCING OLD/NEW TABLE) `[parallel-safe]` `depends: T-241`

### 3F — Stored Routines (PSM)

- [ ] **T-250** — CREATE FUNCTION (SQL PSM) `[serial]` `depends: M2`
- [ ] **T-251** — CREATE PROCEDURE + CALL `[parallel-safe]` `depends: T-250`
- [ ] **T-252** — PSM control flow: IF, CASE, LOOP, WHILE, REPEAT, FOR `[serial]` `depends: T-250`
- [ ] **T-253** — Local variables, cursors, condition handlers `[parallel-safe]` `depends: T-252`
- [ ] **T-254** — SIGNAL / RESIGNAL `[parallel-safe]` `depends: T-252`

### 3G — Compliance

- [ ] **T-260** — SQL:1999 compliance suite (300+ tests) `[parallel-safe]` `depends: T-211,T-222,T-243`
- [ ] **T-261** — Regression gate for SQL-92 suite (no regressions) `[serial]` `depends: T-260`

---
**MILESTONE M3: SQL:1999 Complete**

---

## Phase 4 — SQL:2003 + Wire Protocol `milestone: M4`

- [ ] **T-300** — Window functions: all SPEC §8.3 functions `[serial]` `depends: M3`
- [ ] **T-301** — MERGE statement `[serial]` `depends: M3`
- [ ] **T-302** — MULTISET types `[parallel-safe]` `depends: M3`
- [ ] **T-303** — Generated columns (GENERATED ALWAYS AS ... STORED) `[parallel-safe]` `depends: M3`
- [ ] **T-304** — Identity columns (GENERATED AS IDENTITY) `[parallel-safe]` `depends: M3`
- [ ] **T-305** — Sequence objects: CREATE SEQUENCE, NEXT VALUE FOR `[parallel-safe]` `depends: M3`
- [ ] **T-306** — SQL/XML basic support (XMLTYPE, XMLQUERY, XMLTABLE) `[serial]` `depends: M3`
- [ ] **T-307** — RETURNING clause on INSERT / UPDATE / DELETE `[parallel-safe]` `depends: M3`
- [ ] **T-308** — PostgreSQL wire protocol v3 (query + extended message flow) `[serial]` `depends: M3`
  - Read/query path first; then write path
- [ ] **T-309** — Materialized views `[parallel-safe]` `depends: M3`
- [ ] **T-310** — Deferred constraints `[parallel-safe]` `depends: M3`
- [ ] **T-311** — SQL:2003 compliance suite (200+ tests) `[serial]` `depends: T-300,T-301`
- [ ] **T-312** — Regression gate `[serial]` `depends: T-311`

---
**MILESTONE M4: SQL:2003 + Wire Protocol**

---

## Phase 5 — SQL:2008 + Full-Text `milestone: M5`

- [ ] **T-350** — FETCH FIRST n ROWS ONLY / OFFSET (standard LIMIT replacement) `[parallel-safe]` `depends: M4`
- [ ] **T-351** — TABLESAMPLE clause `[parallel-safe]` `depends: M4`
- [ ] **T-352** — REGEXP_LIKE, REGEXP_REPLACE, REGEXP_SUBSTR (SPEC §8.6) `[parallel-safe]` `depends: M4`
- [ ] **T-353** — Truncate restart identity option `[parallel-safe]` `depends: M4`
- [ ] **T-354** — Full-text search: tsvector, tsquery, GIN index (SPEC §15) `[serial]` `depends: M4`
- [ ] **T-355** — INSTEAD OF trigger on updatable view improvements `[parallel-safe]` `depends: M4`
- [ ] **T-356** — SQL:2008 compliance suite (150+ tests) `[serial]` `depends: T-350,T-352`

---
**MILESTONE M5: SQL:2008**

---

## Phase 6 — SQL:2011 (Temporal) + SQL:2016 (JSON + Row Pattern) `milestone: M6`

- [ ] **T-400** — System-time temporal tables (SPEC §17) `[serial]` `depends: M5`
- [ ] **T-401** — Application-time temporal tables `[parallel-safe]` `depends: T-400`
- [ ] **T-402** — Temporal predicates in FROM clause `[serial]` `depends: T-400`
- [ ] **T-403** — Window frame GROUPS mode, EXCLUDE clause (SQL:2011) `[parallel-safe]` `depends: M5`
- [ ] **T-404** — CTE materialization hints: MATERIALIZED / NOT MATERIALIZED `[parallel-safe]` `depends: M5`
- [ ] **T-410** — JSON type and storage (SPEC §16) `[serial]` `depends: M5`
- [ ] **T-411** — JSON scalar functions: JSON_VALUE, JSON_QUERY, JSON_EXISTS `[parallel-safe]` `depends: T-410`
- [ ] **T-412** — JSON_TABLE `[serial]` `depends: T-411`
- [ ] **T-413** — JSON_OBJECT, JSON_ARRAY, JSON_ARRAYAGG, JSON_OBJECTAGG `[parallel-safe]` `depends: T-411`
- [ ] **T-414** — IS JSON predicate `[parallel-safe]` `depends: T-410`
- [ ] **T-415** — SQL/JSON path language engine `[serial]` `depends: T-410`
- [ ] **T-420** — MATCH_RECOGNIZE row pattern recognition (SPEC §18) `[serial]` `depends: M5`
- [ ] **T-430** — SQL:2011 + SQL:2016 compliance suite (250+ tests) `[serial]` `depends: T-402,T-412,T-420`

---
**MILESTONE M6: SQL:2011 + SQL:2016**

---

## Phase 7 — SQL:2023 Full Compliance `milestone: M7`

- [ ] **T-500** — Property graph queries (SQL/PGQ) — GRAPH_TABLE, path patterns `[serial]` `depends: M6`
- [ ] **T-501** — JSON_SERIALIZE, JSON_SCALAR (SQL:2023 additions) `[parallel-safe]` `depends: M6`
- [ ] **T-502** — ANY_VALUE aggregate function `[parallel-safe]` `depends: M6`
- [ ] **T-503** — Dynamic parameters in more positions `[parallel-safe]` `depends: M6`
- [ ] **T-504** — Lateral join improvements `[parallel-safe]` `depends: M6`
- [ ] **T-505** — UNIQUE predicate (null-distinct uniqueness) `[parallel-safe]` `depends: M6`
- [ ] **T-506** — Full SQL:2023 compliance suite (500+ tests) `[serial]` `depends: T-500,T-501`
- [ ] **T-507** — Full regression suite: SQL-92 through SQL:2023 (2000+ tests green) `[serial]` `depends: T-506`

---
**MILESTONE M7: Full SQL:2023 Compliance** — tucotuco's SQL engine is feature-complete.

---

## Phase 8 — Agentic Layer `milestone: M8`

### 8A — MCP Server (SPEC §20)

- [ ] **T-600** — MCP server scaffold: stdio transport `[serial]` `depends: M7`
- [ ] **T-601** — Tool: `sql_query` (read-only SELECT) `[parallel-safe]` `depends: T-600`
- [ ] **T-602** — Tool: `sql_execute` (INSERT/UPDATE/DELETE/DDL) `[parallel-safe]` `depends: T-600`
- [ ] **T-603** — Tool: `schema_inspect` (list tables, columns, types) `[parallel-safe]` `depends: T-600`
- [ ] **T-604** — Tool: `explain_query` `[parallel-safe]` `depends: T-600`
- [ ] **T-605** — HTTP/SSE transport `[serial]` `depends: T-601,T-602`
- [ ] **T-606** — MCP resource listing: expose schema as resources `[parallel-safe]` `depends: T-603`
- [ ] **T-607** — API key + OAuth2 auth for HTTP transport `[serial]` `depends: T-605`

### 8B — Skills Layer (SPEC §21)

- [ ] **T-620** — `CREATE SKILL` DDL parsing + catalog `[serial]` `depends: M7`
- [ ] **T-621** — Go-language skill executor (sandboxed via plugin or subprocess) `[serial]` `depends: T-620`
- [ ] **T-622** — LLM-backed skill executor (calls Anthropic / OpenAI API) `[serial]` `depends: T-620`
- [ ] **T-623** — Skill caching for DETERMINISTIC skills `[parallel-safe]` `depends: T-622`
- [ ] **T-624** — Skills catalogue in docs `[parallel-safe]` `depends: T-622`

### 8C — External Integrations (SPEC §22)

- [ ] **T-630** — Foreign table interface: `ForeignTableScanner` `[serial]` `depends: M7`
- [ ] **T-631** — CSV foreign table server `[parallel-safe]` `depends: T-630`
- [ ] **T-632** — JSON Lines foreign table server `[parallel-safe]` `depends: T-630`
- [ ] **T-633** — Parquet foreign table server (via `apache/arrow`) `[parallel-safe]` `depends: T-630`
- [ ] **T-634** — REST foreign table server `[parallel-safe]` `depends: T-630`
- [ ] **T-635** — S3 object store wrapper `[parallel-safe]` `depends: T-631,T-633`
- [ ] **T-636** — DuckDB interchange via Arrow IPC / ADBC `[serial]` `depends: T-633`

### 8D — Documentation & Release

- [ ] **T-640** — `docs/getting-started.md` full rewrite `[parallel-safe]` `depends: T-605`
- [ ] **T-641** — `docs/api.md` complete Go embedding reference `[parallel-safe]` `depends: T-605`
- [ ] **T-642** — `docs/sql-support.md` feature matrix (auto-generated from compliance suite) `[parallel-safe]` `depends: T-507`
- [ ] **T-643** — `docs/architecture.md` deep-dive `[parallel-safe]` `depends: M7`
- [ ] **T-644** — v1.0.0 release checklist: semver, changelog, GitHub release `[serial]` `depends: T-640,T-641,T-642,T-636`

---
**MILESTONE M8: Agentic Layer + v1.0.0**

---

## Dependency Summary (Critical Path)

```
M0 → T-010..T-046 → T-050..T-063 → T-070..T-099 → T-100 → M1
M1 → T-120..T-128 (storage) ─┐
M1 → T-130..T-143 (SQL)     ─┤→ M2
M1 → T-150..T-155 (constraints)─┤
M1 → T-160..T-165 (indexes) ─┘
M2 → T-200..T-261 → M3
M3 → T-300..T-312 → M4
M4 → T-350..T-356 → M5
M5 → T-400..T-430 → M6
M6 → T-500..T-507 → M7
M7 → T-600..T-644 → M8
```
