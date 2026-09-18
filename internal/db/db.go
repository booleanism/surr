package db

import (
	"context"
	"database/sql"
	"sync"
)

type DB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type TX interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Transaction, func(), error)
	Commit() error
	Rollback() error
}

type TXFactoryFunc func() *Transaction

func TXFactory(db *sql.DB) func() *Transaction {
	return func() *Transaction {
		return &Transaction{
			DB: db,
		}
	}
}

type Transaction struct {
	dbptr *sql.DB
	DB    DB
	mu    sync.Mutex
}

func (t *Transaction) BeginTx(ctx context.Context, opts *sql.TxOptions) (func(), error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if db, ok := t.DB.(*sql.DB); ok {
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		t.dbptr = db
		t.DB = tx
		return func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			t.DB = t.dbptr
			t.dbptr = nil
		}, nil
	}

	return func() {}, nil
}

func (t *Transaction) Commit() error {
	if tx, ok := t.DB.(*sql.Tx); ok {
		return tx.Commit()
	}

	return nil
}

func (t *Transaction) Rollback() error {
	if tx, ok := t.DB.(*sql.Tx); ok {
		return tx.Rollback()
	}

	return nil
}
