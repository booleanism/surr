//go:build !postgres

// The default build is SQLite. A second engine is selected with a build tag, so
// exactly one dialect is compiled in and there is no runtime switch to get wrong
// -- the same rule tx follows for its encoders (AGENTS.md: "Both tx encoder build
// tags must compile").
//
// The driver is imported HERE, not by the caller. That is the point of this file
// owning DriverName: before the seam existed, RunMigrate named "sqlite3" while the
// registration happened in each service's main.go, so the shared package compiled
// against a driver it did not link and would have failed at run time with
// `unknown driver "sqlite3"` if a caller ever stopped blank-importing it.

package db

import (
	"context"
	"database/sql"
	"errors"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// sqliteDialect is the SQLite implementation. Every string in it is a fact about
// SQLite, and each is a runtime (not compile-time) failure on another engine.
type sqliteDialect struct{}

func (sqliteDialect) DriverName() string { return "sqlite3" }

// CreateVersionTable keeps golang-migrate's column types. `uint64` is not a type
// Postgres accepts, which is exactly why it is here and not in the runner.
func (sqliteDialect) CreateVersionTable() string {
	return "CREATE TABLE IF NOT EXISTS schema_migrations (version uint64, dirty bool)"
}

// BeginLocking uses BEGIN IMMEDIATE so the write lock is taken now rather than on
// the first statement: two migrators starting together would otherwise both read
// the same current version and both try to apply the same migration.
func (sqliteDialect) BeginLocking(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout = 10000"); err != nil {
		return err
	}

	_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")

	return err
}

// MultiStatement is true because the sqlite3 driver executes every statement in
// the string it is handed.
func (sqliteDialect) MultiStatement() bool { return true }

// TransactionalDDL is true: SQLite rolls DDL back with everything else, which is
// why a failed migration leaves the schema untouched and `dirty` is never written
// as true by this runner.
func (sqliteDialect) TransactionalDDL() bool { return true }

func (sqliteDialect) BindVar(int) string { return "?" }

// IsUniqueViolation matches the EXTENDED code, not the primary one: SQLITE_CONSTRAINT
// also covers NOT NULL, CHECK and foreign-key failures, so the primary code would
// report a missing column as a duplicate row.
func (sqliteDialect) IsUniqueViolation(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}

	return sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique
}

// dialectFor is the single point where the build tag picks an implementation.
func dialectFor() dialect { return sqliteDialect{} }
