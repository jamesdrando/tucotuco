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

func TestQueryRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	db := mustOpenDB(t, filepath.Join(t.TempDir(), "catalog.json"))

	_, err := db.Query("SELECT 1 ORDER BY 1")
	sqlErr := assertSQLError(t, err)
	if got, want := sqlErr.Diagnostics[0].SQLState, sqlStateFeatureNotSupported; got != want {
		t.Fatalf("sqlErr.Diagnostics[0].SQLState = %q, want %q", got, want)
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
