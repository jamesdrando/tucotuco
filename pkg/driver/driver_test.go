package driver_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/jamesdrando/tucotuco/pkg/driver"
	"github.com/jamesdrando/tucotuco/pkg/embed"
)

func TestRegistrationAndOpen(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()
}

func TestExecAndQueryHappyPath(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20))"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	result, err := db.Exec("INSERT INTO widgets VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("Exec(INSERT) error = %v", err)
	}
	if got, want := mustRowsAffected(t, result), int64(1); got != want {
		t.Fatalf("INSERT RowsAffected = %d, want %d", got, want)
	}

	rows, err := db.Query("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(SELECT) error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}

	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if got, want := id, int64(1); got != want {
		t.Fatalf("id = %d, want %d", got, want)
	}
	if got, want := name, "alice"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestExplicitTxCommitAndRollback(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20))"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	result, err := tx.Exec("INSERT INTO widgets VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("tx.Exec(INSERT) error = %v", err)
	}
	if got, want := mustRowsAffected(t, result), int64(1); got != want {
		t.Fatalf("tx INSERT RowsAffected = %d, want %d", got, want)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit() error = %v", err)
	}

	var committedID int64
	if err := db.QueryRow("SELECT id FROM widgets WHERE id = 1").Scan(&committedID); err != nil {
		t.Fatalf("QueryRow(committed row) error = %v", err)
	}
	if got, want := committedID, int64(1); got != want {
		t.Fatalf("committedID = %d, want %d", got, want)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO widgets VALUES (2, 'bob')"); err != nil {
		t.Fatalf("tx.Exec(second INSERT) error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx.Rollback() error = %v", err)
	}

	if err := db.QueryRow("SELECT id FROM widgets WHERE id = 2").Scan(&committedID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("QueryRow(rolled back row) error = %v, want sql.ErrNoRows", err)
	}
}

func TestBeginTxRejectsNonDefaultOptions(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err == nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		t.Fatal("BeginTx(ReadOnly) error = nil, want rejection")
	}

	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		t.Fatal("BeginTx(Serializable) error = nil, want rejection")
	}
}

func TestPrepareCompatibilityShim(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20))"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO widgets VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("Prepare(INSERT) error = %v", err)
	}
	defer stmt.Close()

	result, err := stmt.Exec()
	if err != nil {
		t.Fatalf("stmt.Exec() error = %v", err)
	}
	if got, want := mustRowsAffected(t, result), int64(1); got != want {
		t.Fatalf("stmt.Exec RowsAffected = %d, want %d", got, want)
	}

	stmt, err = db.Prepare("SELECT id, name FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("Prepare(SELECT) error = %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("stmt.Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("stmt.Query rows.Next() = false, want true")
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("stmt.Query rows.Scan() error = %v", err)
	}
	if got, want := id, int64(1); got != want {
		t.Fatalf("stmt.Query id = %d, want %d", got, want)
	}
	if got, want := name, "alice"; got != want {
		t.Fatalf("stmt.Query name = %q, want %q", got, want)
	}
}

func TestDatabaseSQLErrorPropagation(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	if _, err := db.Query("SELECT 1 ORDER BY 1"); err == nil {
		t.Fatal("Query(unsupported shape) error = nil, want SQLError")
	} else {
		assertFeatureNotSupported(t, err)
	}
}

func TestUnsupportedBindParameters(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLDB(t, filepath.Join(t.TempDir(), "catalog.json"))
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE widgets (id INTEGER NOT NULL)"); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO widgets VALUES (?)", 1); err == nil {
		t.Fatal("Exec with bind parameter error = nil, want SQLError")
	} else {
		assertFeatureNotSupported(t, err)
	}

	if _, err := db.Query("SELECT ?", 1); err == nil {
		t.Fatal("Query with bind parameter error = nil, want SQLError")
	} else {
		assertFeatureNotSupported(t, err)
	}
}

func mustOpenSQLDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("tucotuco", path)
	if err != nil {
		t.Fatalf(`sql.Open("tucotuco", %q) error = %v`, path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping() error = %v", err)
	}

	return db
}

func mustRowsAffected(t *testing.T, result sql.Result) int64 {
	t.Helper()

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected() error = %v", err)
	}

	return rowsAffected
}

func assertFeatureNotSupported(t *testing.T, err error) {
	t.Helper()

	var sqlErr *embed.SQLError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("error = %T, want *embed.SQLError", err)
	}
	if len(sqlErr.Diagnostics) == 0 {
		t.Fatalf("sqlErr.Diagnostics = %#v, want at least one diagnostic", sqlErr.Diagnostics)
	}
	if got, want := sqlErr.Diagnostics[0].SQLState, "0A000"; got != want {
		t.Fatalf("sqlErr.Diagnostics[0].SQLState = %q, want %q", got, want)
	}
}
