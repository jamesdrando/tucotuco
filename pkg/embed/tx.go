package embed

import (
	"github.com/jamesdrando/tucotuco/internal/storage"
)

// Tx is one explicit embed transaction.
type Tx struct {
	db      *DB
	tx      storage.Transaction
	session *session

	closed bool
}

// Exec executes one non-query SQL statement inside the explicit transaction.
func (tx *Tx) Exec(sql string) (CommandResult, error) {
	if err := tx.requireOpen(); err != nil {
		return CommandResult{}, err
	}

	outcome, err := tx.session.exec(sql)
	if err != nil {
		return CommandResult{}, wrapSQLError(err)
	}

	return outcome.result, nil
}

// Query executes one SQL query inside the explicit transaction.
func (tx *Tx) Query(sql string) (*ResultSet, error) {
	if err := tx.requireOpen(); err != nil {
		return nil, err
	}

	result, err := tx.session.query(sql)
	if err != nil {
		return nil, wrapSQLError(err)
	}

	return cloneResultSet(result), nil
}

// Commit commits the explicit transaction and releases the DB-level
// transaction gate.
func (tx *Tx) Commit() error {
	if err := tx.requireOpen(); err != nil {
		return err
	}
	if err := tx.tx.Commit(); err != nil {
		return err
	}

	tx.finish()
	return nil
}

// Rollback rolls back the explicit transaction and releases the DB-level
// transaction gate.
func (tx *Tx) Rollback() error {
	if err := tx.requireOpen(); err != nil {
		return err
	}
	if err := tx.tx.Rollback(); err != nil {
		return err
	}

	tx.finish()
	return nil
}

func (tx *Tx) requireOpen() error {
	switch {
	case tx == nil:
		return errNilTx
	case tx.closed || tx.tx == nil || tx.session == nil:
		return errTxClosed
	default:
		return nil
	}
}

func (tx *Tx) finish() {
	if tx == nil || tx.closed {
		return
	}

	tx.closed = true
	tx.session = nil
	tx.tx = nil

	if tx.db != nil {
		tx.db.gate.Unlock()
	}
}
