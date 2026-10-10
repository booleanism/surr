package db

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// ErrTxInProgress is returned when BeginTx is called on a Transaction that
// already has an un-released transaction. It is unreachable in practice
// because BeginTx serialises callers through an internal gate, but it keeps
// the failure explicit instead of silently joining a sibling transaction.
var ErrTxInProgress = errors.New("transaction already in progress")

type DB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type TX interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (func(), error)
	Commit() error
	Rollback() error
}

type TXFactoryFunc func() *Transaction

func TXFactory(db *sql.DB) func() *Transaction {
	return func() *Transaction {
		return &Transaction{
			DB:   db,
			gate: make(chan struct{}, 1),
		}
	}
}

// Transaction swaps a *sql.DB for a *sql.Tx for the duration of a multi
// statement write, then swaps it back.
//
// A Transaction is safe for concurrent use. BeginTx blocks until any in-flight
// transaction on the same value has been released, so two overlapping callers
// each get their own *sql.Tx instead of the second one silently executing on
// the first one's transaction (which produced lost writes and "database is
// locked" errors).
type Transaction struct {
	dbptr *sql.DB
	DB    DB
	gate  chan struct{}
	mu    sync.Mutex
}

// BeginTx starts a transaction and returns the release function that restores
// the underlying *sql.DB. The release function is idempotent and must be
// called even when BeginTx reports an error.
func (t *Transaction) BeginTx(ctx context.Context, opts *sql.TxOptions) (func(), error) {
	gate := t.gateOrCreate()

	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}

	t.mu.Lock()
	db, ok := t.DB.(*sql.DB)
	t.mu.Unlock()

	if !ok {
		<-gate
		return func() {}, ErrTxInProgress
	}

	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		<-gate
		return func() {}, err
	}

	t.mu.Lock()
	t.dbptr = db
	t.DB = tx
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			t.DB = t.dbptr
			t.dbptr = nil
			t.mu.Unlock()

			<-gate
		})
	}, nil
}

// gateOrCreate lazily allocates the serialisation gate so that a Transaction
// built without TXFactory (zero value) still behaves.
func (t *Transaction) gateOrCreate() chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.gate == nil {
		t.gate = make(chan struct{}, 1)
	}

	return t.gate
}

func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if tx, ok := t.DB.(*sql.Tx); ok {
		return tx.Commit()
	}

	return nil
}

func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if tx, ok := t.DB.(*sql.Tx); ok {
		return tx.Rollback()
	}

	return nil
}
