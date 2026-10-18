package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

// newTestUsers returns a users store over an in-memory database carrying the
// users table exactly as the migration creates it, so the UNIQUE constraint the
// translation depends on is the real one rather than a stand-in.
func newTestUsers(t *testing.T) users {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	_, err = sqlDB.Exec(`CREATE TABLE users (
		id BLOB PRIMARY KEY,
		email VARCHAR(64) UNIQUE NOT NULL,
		passwd VARCHAR(64) NOT NULL
	)`)
	if err != nil {
		t.Fatalf("create users table: %v", err)
	}

	return users{Transaction: &db.Transaction{DB: sqlDB}}
}

func TestInsertUserReportsADuplicateEmail(t *testing.T) {
	store := newTestUsers(t)
	ctx := context.Background()

	first := models.User{ID: uuid.New(), Email: "a@b.c", Passwd: "hash"}
	if err := store.InsertUser(ctx, first); err != nil {
		t.Fatalf("insert first user: %v", err)
	}

	second := models.User{ID: uuid.New(), Email: "a@b.c", Passwd: "hash"}

	err := store.InsertUser(ctx, second)
	if !errors.Is(err, models.ErrUserExists) {
		t.Fatalf("duplicate insert returned %v, want ErrUserExists", err)
	}
}

// SQLITE_CONSTRAINT covers NOT NULL, CHECK and foreign key failures as well as
// UNIQUE, so matching the primary code would report a missing field as a
// duplicate account. The driver error is built by hand because the extended
// code is the whole subject of the test and the users table has no other
// constraint to trip.
func TestIsUniqueViolationOnlyMatchesUniqueFailures(t *testing.T) {
	unique := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintUnique}
	if !db.IsUniqueViolation(unique) {
		t.Error("a UNIQUE failure was not recognised")
	}

	for _, extended := range []sqlite3.ErrNoExtended{
		sqlite3.ErrConstraintNotNull,
		sqlite3.ErrConstraintCheck,
		sqlite3.ErrConstraintForeignKey,
	} {
		other := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: extended}
		if db.IsUniqueViolation(other) {
			t.Errorf("%v was reported as a duplicate", extended)
		}
	}

	if db.IsUniqueViolation(errors.New("no such table: users")) {
		t.Error("a non-driver error was reported as a duplicate")
	}
}
