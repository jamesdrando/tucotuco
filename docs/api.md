# API

`pkg/embed` is the current embeddable Phase 1 API.

`pkg/driver` registers the `database/sql` driver name `tucotuco` and maps it
to the same Phase 1 engine.

```go
db, err := embed.Open("/path/to/catalog.json")
result, err := db.Exec("CREATE TABLE widgets (id INTEGER, name VARCHAR(20))")
rows, err := db.Query("SELECT id, name FROM widgets")
tx, err := db.Begin()
```

Current surface:

- `Open(path) (*DB, error)`
- `(*DB).Exec(sql string) (CommandResult, error)`
- `(*DB).Query(sql string) (*ResultSet, error)`
- `(*DB).Begin() (*Tx, error)`
- `(*Tx).Exec(sql string) (CommandResult, error)`
- `(*Tx).Query(sql string) (*ResultSet, error)`
- `(*Tx).Commit() error`
- `(*Tx).Rollback() error`

Returned types:

- `CommandResult` exposes `RowsAffected`
- `ResultSet` eagerly materializes `Columns` and `Rows`
- `Column.Type` uses canonical SQL text such as `INTEGER` or `VARCHAR(20)`
- SQL failures return `*SQLError` with structured diagnostics (`SQLSTATE`, message, and position)

Current execution scope is intentionally small and matches the existing planner and executor bridge:

- `Query` supports one `SELECT` statement at a time
- supported query shapes are the current planner subset plus `SELECT` without `FROM`
- `Exec` supports one statement at a time from: `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`
- `DB.Begin` opens an explicit DML/query transaction; `DB`-level `Exec`, `Query`, and `Begin` block while it is active

Current limitations:

- row storage is still in-memory only; `Open(path)` and `sql.Open("tucotuco", path)` persist catalog metadata, not table contents
- SQL transaction-control statements (`BEGIN`, `COMMIT`, `ROLLBACK`) are rejected; use the Go transaction API instead
- planner-driven query execution still rejects `ORDER BY`, `GROUP BY`, `HAVING`, `DISTINCT`, joins, and `LIMIT/OFFSET`
- `INSERT DEFAULT VALUES` and omitted columns that would require runtime `DEFAULT`, generated, or identity synthesis are rejected with feature-not-supported errors
- DDL inside explicit `Tx` is not supported in Phase 1
