package sqlite

import (
	"errors"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// isUniqueViolation reports whether err is SQLite refusing a duplicate value
// for a UNIQUE column.
//
// The extended code is what is checked, not the primary one: SQLITE_CONSTRAINT
// also covers NOT NULL, CHECK and foreign key failures, so matching on it would
// report a missing column as a duplicate row.
//
// It is duplicated from hub/internal/db/sqlite rather than shared: the root
// module deliberately does not depend on the sqlite driver, so putting it in
// internal/db would drag a cgo driver into the dependency graph of every
// service to save ten lines.
func isUniqueViolation(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}

	return sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique
}
