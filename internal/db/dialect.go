package db

import (
	"context"
	"database/sql"
)

// dialect is the part of a migration run that depends on the engine.
//
// The engine is chosen at BUILD time, not at run time: exactly one implementation
// of this interface is compiled into a binary, selected by a build tag, the same
// way tx selects its encoder (see the "both build tags must compile" invariant in
// AGENTS.md). There is deliberately no switch on a driver name anywhere -- a
// runtime choice would mean every binary carries every engine's SQL and every
// engine's DDL, and the wrong one is a data-loss bug that only shows up when a
// flag is wrong.
//
// Why this is an interface at all rather than one SQLite file: a migration
// runner's portability is five strings and one boolean, and they are exactly the
// five things that break silently when copied. Collecting them here means a
// second engine is a new file implementing this interface, not an audit of the
// runner.
type dialect interface {
	// DriverName is the database/sql driver the runner opens. The implementation
	// registers it, so a binary cannot compile against a driver it does not link.
	DriverName() string

	// CreateVersionTable is the DDL for the bookkeeping table, idempotent. The
	// column types are golang-migrate's, deliberately: this table is the contract
	// between that tool and this runner, and a database migrated by one must stay
	// readable by the other.
	CreateVersionTable() string

	// BeginLocking opens a transaction that holds the write lock immediately,
	// rather than taking it on the first write. Two migrators starting at once
	// would otherwise both read the same current version and both apply the same
	// migration.
	BeginLocking(ctx context.Context, conn *sql.Conn) error

	// MultiStatement reports whether one Exec may carry a whole migration file.
	// SQLite's driver runs every statement in the string; a driver that does not
	// needs the file split, and a splitter that misreads a semicolon inside a
	// string literal or a trigger body corrupts the migration -- which is why this
	// is a capability the dialect states rather than a guess the runner makes.
	MultiStatement() bool

	// TransactionalDDL reports whether a failed migration can be rolled back. Where
	// it cannot, a failure leaves the schema half-applied and the runner has to say
	// so instead of pretending the rollback restored something.
	TransactionalDDL() bool

	// BindVar renders the n-th (1-based) query parameter. SQLite and MySQL write
	// "?", Postgres writes "$1"; a hardcoded "?" is a runtime error on Postgres,
	// not a compile error, so it belongs here with the rest.
	BindVar(n int) string
}

// StatementSplitter is implemented by a dialect whose driver refuses a
// multi-statement Exec. It returns the statements in body, in order.
//
// None of today's dialects need it -- SQLite runs the file whole -- but the runner
// checks for it so a future engine supplies its own parser rather than relying on
// a generic split on ';'.
type statementSplitter interface {
	SplitStatements(body string) []string
}
