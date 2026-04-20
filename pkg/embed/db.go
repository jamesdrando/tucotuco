package embed

import (
	"errors"
	"sync"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/memory"
)

const defaultSchemaName = "public"

// DB is one embeddable tucotuco database handle.
type DB struct {
	path  string
	cat   *catalog.Memory
	store *memory.Store

	gate      sync.RWMutex
	execMu    sync.Mutex
	persistMu sync.Mutex
}

// Open loads the catalog metadata stored at path, bootstraps the public
// schema, and returns a database handle backed by an in-memory row store.
func Open(path string) (*DB, error) {
	cat, err := catalog.LoadFile(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		path:  path,
		cat:   cat,
		store: memory.New(),
	}

	created, err := db.ensureDefaultSchema()
	if err != nil {
		return nil, err
	}
	if created {
		if err := db.persistCatalog(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// Exec executes one non-query SQL statement in its own autocommit transaction.
func (db *DB) Exec(sql string) (CommandResult, error) {
	if db == nil {
		return CommandResult{}, errNilDB
	}

	db.gate.RLock()
	defer db.gate.RUnlock()

	db.execMu.Lock()
	defer db.execMu.Unlock()

	tx, err := db.store.NewTransaction(storage.TransactionOptions{})
	if err != nil {
		return CommandResult{}, err
	}

	outcome, err := newSession(db.cat, db.store, tx, true).exec(sql)
	if err != nil {
		return CommandResult{}, joinErrors(wrapSQLError(err), tx.Rollback())
	}

	if err := tx.Commit(); err != nil {
		return outcome.result, joinErrors(err, tx.Rollback())
	}
	if outcome.catalogChanged {
		if err := db.persistCatalog(); err != nil {
			return outcome.result, err
		}
	}

	return outcome.result, nil
}

// Query executes one SQL query in its own autocommit read-only transaction.
func (db *DB) Query(sql string) (*ResultSet, error) {
	if db == nil {
		return nil, errNilDB
	}

	db.gate.RLock()
	defer db.gate.RUnlock()

	tx, err := db.store.NewTransaction(storage.TransactionOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	result, err := newSession(db.cat, db.store, tx, true).query(sql)
	if err != nil {
		return nil, joinErrors(wrapSQLError(err), tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return nil, joinErrors(err, tx.Rollback())
	}

	return cloneResultSet(result), nil
}

// Begin starts one explicit transaction and blocks DB-level Exec, Query, and
// Begin calls until the transaction is committed or rolled back.
func (db *DB) Begin() (*Tx, error) {
	if db == nil {
		return nil, errNilDB
	}

	db.gate.Lock()

	tx, err := db.store.NewTransaction(storage.TransactionOptions{})
	if err != nil {
		db.gate.Unlock()
		return nil, err
	}

	return &Tx{
		db:      db,
		tx:      tx,
		session: newSession(db.cat, db.store, tx, false),
	}, nil
}

func (db *DB) ensureDefaultSchema() (bool, error) {
	if err := db.cat.CreateSchema(&catalog.SchemaDescriptor{Name: defaultSchemaName}); err != nil {
		if errors.Is(err, catalog.ErrSchemaExists) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (db *DB) persistCatalog() error {
	db.persistMu.Lock()
	defer db.persistMu.Unlock()

	return catalog.SaveFile(db.path, db.cat)
}

type execOutcome struct {
	result         CommandResult
	catalogChanged bool
}

type session struct {
	cat      *catalog.Memory
	store    *memory.Store
	tx       storage.Transaction
	allowDDL bool
}

func newSession(cat *catalog.Memory, store *memory.Store, tx storage.Transaction, allowDDL bool) *session {
	return &session{
		cat:      cat,
		store:    store,
		tx:       tx,
		allowDDL: allowDDL,
	}
}
