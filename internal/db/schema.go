package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrSchemaMissing reports a database that has not been migrated.
var ErrSchemaMissing = errors.New("database schema is missing")

// VerifySchema fails when database does not have every named table.
//
// The migrations live in the repo as SQL and are applied out-of-band with the
// golang-migrate CLI, so nothing in a service's startup path notices a fresh
// checkout: hub and subscriptions would open an empty file, start happily and
// then fail one "no such table" at a time on the first requests, with no hint
// that a migration step had been skipped. Called right after the connection is
// opened, this turns that into a single actionable error.
//
// migrationsPath is only used to build that error message.
func VerifySchema(ctx context.Context, database *sql.DB, migrationsPath string, tables ...string) error {
	missing := make([]string, 0, len(tables))

	for _, table := range tables {
		ok, err := tableExists(ctx, database, table)
		if err != nil {
			return fmt.Errorf("checking for table %s: %w", table, err)
		}

		if !ok {
			missing = append(missing, table)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: missing table(s) %s; apply the migrations first: migrate -database sqlite3://<db file> -path %s up",
		ErrSchemaMissing,
		strings.Join(missing, ", "),
		migrationsPath,
	)
}

// tableExists asks sqlite_master rather than selecting from the table itself:
// the name is a parameter, so this never interpolates a table name into SQL.
func tableExists(ctx context.Context, database *sql.DB, name string) (bool, error) {
	var found string

	err := database.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", name,
	).Scan(&found)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
