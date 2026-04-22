package embed

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestOpenBootstrapsPublicSchemaAndPersistsCatalogMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "catalog.json")
	db := mustOpenDB(t, path)

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER)"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	reopened := mustOpenDB(t, path)
	result, err := reopened.Query("SELECT * FROM widgets")
	if err != nil {
		t.Fatalf("Query(reopened table) error = %v", err)
	}

	if got, want := len(result.Columns), 1; got != want {
		t.Fatalf("len(result.Columns) = %d, want %d", got, want)
	}
	if got, want := result.Columns[0].Name, "id"; got != want {
		t.Fatalf("result.Columns[0].Name = %q, want %q", got, want)
	}
	if got, want := result.Columns[0].Type, "INTEGER"; got != want {
		t.Fatalf("result.Columns[0].Type = %q, want %q", got, want)
	}
	if got := len(result.Rows); got != 0 {
		t.Fatalf("len(result.Rows) = %d, want 0", got)
	}
}

func TestExecAndQueryEndToEnd(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))

	literal, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query(SELECT 1) error = %v", err)
	}
	if got, want := literal.Rows, [][]any{{int32(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("literal.Rows = %#v, want %#v", got, want)
	}
	if got, want := literal.Columns[0].Type, "INTEGER NOT NULL"; got != want {
		t.Fatalf("literal.Columns[0].Type = %q, want %q", got, want)
	}

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20))"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}
	if result, err := db.Exec("INSERT INTO widgets VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(INSERT) error = %v", err)
	} else if got, want := result.RowsAffected, int64(1); got != want {
		t.Fatalf("INSERT RowsAffected = %d, want %d", got, want)
	}

	selected, err := db.Query("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(selected row) error = %v", err)
	}
	if got, want := selected.Rows, [][]any{{int32(1), "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected.Rows = %#v, want %#v", got, want)
	}

	if result, err := db.Exec("UPDATE widgets SET name = 'bob' WHERE id = 1"); err != nil {
		t.Fatalf("Exec(UPDATE) error = %v", err)
	} else if got, want := result.RowsAffected, int64(1); got != want {
		t.Fatalf("UPDATE RowsAffected = %d, want %d", got, want)
	}

	updated, err := db.Query("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(updated row) error = %v", err)
	}
	if got, want := updated.Rows, [][]any{{int32(1), "bob"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updated.Rows = %#v, want %#v", got, want)
	}

	if result, err := db.Exec("DELETE FROM widgets WHERE id = 1"); err != nil {
		t.Fatalf("Exec(DELETE) error = %v", err)
	} else if got, want := result.RowsAffected, int64(1); got != want {
		t.Fatalf("DELETE RowsAffected = %d, want %d", got, want)
	}

	deleted, err := db.Query("SELECT id FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(after delete) error = %v", err)
	}
	if got := len(deleted.Rows); got != 0 {
		t.Fatalf("len(deleted.Rows) = %d, want 0", got)
	}

	if _, err := db.Exec("DROP TABLE widgets"); err != nil {
		t.Fatalf("Exec(DROP TABLE) error = %v", err)
	}
	if _, err := db.Query("SELECT * FROM widgets"); err == nil {
		t.Fatal("Query(dropped table) error = nil, want SQLError")
	} else {
		_ = assertSQLError(t, err)
	}
}

func TestTransactionCommitAndRollback(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20))"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO widgets VALUES (1, 'alice')"); err != nil {
		t.Fatalf("tx.Exec(INSERT) error = %v", err)
	}

	inTx, err := tx.Query("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("tx.Query() error = %v", err)
	}
	if got, want := inTx.Rows, [][]any{{int32(1), "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("inTx.Rows = %#v, want %#v", got, want)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit() error = %v", err)
	}

	committed, err := db.Query("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(committed row) error = %v", err)
	}
	if got, want := committed.Rows, [][]any{{int32(1), "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed.Rows = %#v, want %#v", got, want)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO widgets VALUES (2, 'bob')"); err != nil {
		t.Fatalf("tx.Exec(second insert) error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx.Rollback() error = %v", err)
	}

	rolledBack, err := db.Query("SELECT id FROM widgets WHERE id = 2")
	if err != nil {
		t.Fatalf("Query(rolled back row) error = %v", err)
	}
	if got := len(rolledBack.Rows); got != 0 {
		t.Fatalf("len(rolledBack.Rows) = %d, want 0", got)
	}
}

func TestDBMethodsBlockedWhileExplicitTxActive(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := db.Query("SELECT 1")
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("db.Query returned while transaction was active: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx.Rollback() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("db.Query after rollback error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("db.Query remained blocked after transaction rollback")
	}
}

func TestQueryExecutesJoinShapesEndToEnd(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	for _, sql := range []string{
		"CREATE TABLE orders (id INTEGER NOT NULL, customer_id INTEGER)",
		"CREATE TABLE customers (customer_id INTEGER NOT NULL, name VARCHAR(20) NOT NULL)",
		"INSERT INTO orders VALUES (1, 10)",
		"INSERT INTO orders VALUES (2, 20)",
		"INSERT INTO orders VALUES (3, NULL)",
		"INSERT INTO customers VALUES (10, 'alice')",
		"INSERT INTO customers VALUES (30, 'carol')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	inner, err := db.Query("SELECT o.id, c.name FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id")
	if err != nil {
		t.Fatalf("Query(inner join) error = %v", err)
	}
	if got, want := inner.Rows, [][]any{{int32(1), "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("inner.Rows = %#v, want %#v", got, want)
	}

	left, err := db.Query("SELECT o.id, c.name FROM orders AS o LEFT JOIN customers AS c ON o.customer_id = c.customer_id")
	if err != nil {
		t.Fatalf("Query(left join) error = %v", err)
	}
	if got, want := left.Rows, [][]any{{int32(1), "alice"}, {int32(2), nil}, {int32(3), nil}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("left.Rows = %#v, want %#v", got, want)
	}
	if got, want := left.Columns[1].Type, "VARCHAR(20)"; got != want {
		t.Fatalf("left.Columns[1].Type = %q, want %q", got, want)
	}

	right, err := db.Query("SELECT o.id, c.name FROM orders AS o RIGHT JOIN customers AS c ON o.customer_id = c.customer_id")
	if err != nil {
		t.Fatalf("Query(right join) error = %v", err)
	}
	if got, want := right.Rows, [][]any{{int32(1), "alice"}, {nil, "carol"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("right.Rows = %#v, want %#v", got, want)
	}

	full, err := db.Query("SELECT o.id, c.name FROM orders AS o FULL OUTER JOIN customers AS c ON o.customer_id = c.customer_id")
	if err != nil {
		t.Fatalf("Query(full join) error = %v", err)
	}
	if got, want := full.Rows, [][]any{{int32(1), "alice"}, {int32(2), nil}, {int32(3), nil}, {nil, "carol"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("full.Rows = %#v, want %#v", got, want)
	}

	cross, err := db.Query("SELECT o.id, c.customer_id FROM orders AS o, customers AS c")
	if err != nil {
		t.Fatalf("Query(cross join) error = %v", err)
	}
	if got, want := cross.Rows, [][]any{
		{int32(1), int32(10)},
		{int32(1), int32(30)},
		{int32(2), int32(10)},
		{int32(2), int32(30)},
		{int32(3), int32(10)},
		{int32(3), int32(30)},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cross.Rows = %#v, want %#v", got, want)
	}
}

func TestQueryRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))

	_, err := db.Query("SELECT 1 ORDER BY 1")
	sqlErr := assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("sqlErr.Diagnostics[0].SQLState = %q, want %q", got, want)
	}

	_, err = db.Query("SELECT 1 FROM (SELECT 1) AS a NATURAL JOIN (SELECT 2) AS b")
	sqlErr = assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].Message, "NATURAL JOIN planning is not supported in Phase 2 planner"; got != want {
		t.Fatalf("NATURAL JOIN diagnostic = %q, want %q", got, want)
	}

	_, err = db.Query("SELECT 1 FROM (SELECT 1 AS customer_id) AS a JOIN (SELECT 2 AS customer_id) AS b USING (customer_id)")
	sqlErr = assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].Message, "JOIN ... USING is not supported in Phase 2 planner"; got != want {
		t.Fatalf("JOIN USING diagnostic = %q, want %q", got, want)
	}

	_, err = db.Query("CREATE TABLE widgets (id INTEGER)")
	sqlErr = assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("query SQLSTATE = %q, want %q", got, want)
	}
}

func TestExecRejectsSelectAndTransactionSQL(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))

	_, err := db.Exec("SELECT 1")
	sqlErr := assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("Exec(SELECT) SQLSTATE = %q, want %q", got, want)
	}

	_, err = db.Exec("BEGIN")
	sqlErr = assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("Exec(BEGIN) SQLSTATE = %q, want %q", got, want)
	}
}

func TestInsertRejectsOmittedDefaultColumnExecution(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20) DEFAULT 'x')"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	_, err := db.Exec("INSERT INTO widgets (id) VALUES (1)")
	sqlErr := assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("Exec(INSERT omitted default) SQLSTATE = %q, want %q", got, want)
	}
}

func TestJoinQueriesEndToEnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]any
	}{
		{
			name: "inner",
			sql: "SELECT l.id, l.label, r.id, r.note " +
				"FROM left_items AS l INNER JOIN right_items AS r ON l.id = r.id",
			want: [][]any{
				{int32(1), "l1", int32(1), "r1"},
				{int32(4), "l4", int32(4), "r4"},
			},
		},
		{
			name: "left",
			sql: "SELECT l.id, l.label, r.id, r.note " +
				"FROM left_items AS l LEFT JOIN right_items AS r ON l.id = r.id",
			want: [][]any{
				{int32(1), "l1", int32(1), "r1"},
				{int32(2), "l2", nil, nil},
				{int32(4), "l4", int32(4), "r4"},
			},
		},
		{
			name: "right",
			sql: "SELECT l.id, l.label, r.id, r.note " +
				"FROM left_items AS l RIGHT JOIN right_items AS r ON l.id = r.id",
			want: [][]any{
				{int32(1), "l1", int32(1), "r1"},
				{int32(4), "l4", int32(4), "r4"},
				{nil, nil, int32(3), "r3"},
			},
		},
		{
			name: "full",
			sql: "SELECT l.id, l.label, r.id, r.note " +
				"FROM left_items AS l FULL JOIN right_items AS r ON l.id = r.id",
			want: [][]any{
				{int32(1), "l1", int32(1), "r1"},
				{int32(2), "l2", nil, nil},
				{int32(4), "l4", int32(4), "r4"},
				{nil, nil, int32(3), "r3"},
			},
		},
		{
			name: "cross",
			sql: "SELECT l.id, l.label, r.id, r.note " +
				"FROM left_items AS l CROSS JOIN right_items AS r",
			want: [][]any{
				{int32(1), "l1", int32(1), "r1"},
				{int32(1), "l1", int32(3), "r3"},
				{int32(1), "l1", int32(4), "r4"},
				{int32(2), "l2", int32(1), "r1"},
				{int32(2), "l2", int32(3), "r3"},
				{int32(2), "l2", int32(4), "r4"},
				{int32(4), "l4", int32(1), "r1"},
				{int32(4), "l4", int32(3), "r3"},
				{int32(4), "l4", int32(4), "r4"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := setupJoinFixture(t)
			result, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query(%q) error = %v", tc.sql, err)
			}

			if got, want := result.Rows, tc.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("result.Rows = %#v, want %#v", got, want)
			}
		})
	}
}

func TestJoinStarExpansionAndAmbiguousUnqualifiedColumns(t *testing.T) {
	t.Parallel()

	db := setupJoinFixture(t)

	result, err := db.Query("SELECT * FROM left_items AS l INNER JOIN right_items AS r ON l.id = r.id")
	if err != nil {
		t.Fatalf("Query(SELECT *) error = %v", err)
	}

	if got, want := []string{
		result.Columns[0].Name,
		result.Columns[1].Name,
		result.Columns[2].Name,
		result.Columns[3].Name,
	}, []string{"id", "label", "id", "note"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result.Columns names = %#v, want %#v", got, want)
	}
	if got, want := result.Rows, [][]any{
		{int32(1), "l1", int32(1), "r1"},
		{int32(4), "l4", int32(4), "r4"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result.Rows = %#v, want %#v", got, want)
	}

	_, err = db.Query("SELECT id FROM left_items AS l INNER JOIN right_items AS r ON l.id = r.id")
	sqlErr := assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, "42702"; got != want {
		t.Fatalf("ambiguous join column SQLSTATE = %q, want %q", got, want)
	}
}

func TestJoinUsingAndNaturalRemainUnsupported(t *testing.T) {
	t.Parallel()

	db := setupJoinFixture(t)

	cases := []string{
		"SELECT * FROM left_items JOIN right_items USING (id)",
		"SELECT * FROM left_items NATURAL JOIN right_items",
	}

	for _, sql := range cases {
		_, err := db.Query(sql)
		sqlErr := assertSQLError(t, err)
		if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
			t.Fatalf("Query(%q) SQLSTATE = %q, want %q", sql, got, want)
		}
	}
}

func setupJoinFixture(t *testing.T) *DB {
	t.Helper()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	mustExecSQL(t, db, "CREATE TABLE left_items (id INTEGER, label VARCHAR(20))")
	mustExecSQL(t, db, "CREATE TABLE right_items (id INTEGER, note VARCHAR(20))")

	mustExecSQL(t, db, "INSERT INTO left_items VALUES (1, 'l1')")
	mustExecSQL(t, db, "INSERT INTO left_items VALUES (2, 'l2')")
	mustExecSQL(t, db, "INSERT INTO left_items VALUES (4, 'l4')")

	mustExecSQL(t, db, "INSERT INTO right_items VALUES (1, 'r1')")
	mustExecSQL(t, db, "INSERT INTO right_items VALUES (3, 'r3')")
	mustExecSQL(t, db, "INSERT INTO right_items VALUES (4, 'r4')")

	return db
}

func mustExecSQL(t *testing.T, db *DB, sql string) {
	t.Helper()

	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("Exec(%q) error = %v", sql, err)
	}
}

func mustOpenDB(t *testing.T, path string) *DB {
	t.Helper()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", path, err)
	}

	return db
}

func assertSQLError(t *testing.T, err error) *SQLError {
	t.Helper()

	if err == nil {
		t.Fatal("error = nil, want SQLError")
	}

	var sqlErr *SQLError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("error = %T, want *SQLError", err)
	}
	if len(sqlErr.Diagnostics) == 0 {
		t.Fatalf("sqlErr.Diagnostics = %#v, want at least one diagnostic", sqlErr.Diagnostics)
	}

	return sqlErr
}
