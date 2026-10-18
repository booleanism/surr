//go:build !postgres

package db

import (
	"context"
	"errors"
	"testing"
)

// TestIsUniqueViolationTellsAUniqueFailureFromANotNullOne is the whole point of
// checking the extended code. SQLITE_CONSTRAINT is the primary code for both, so a
// caller matching on it would answer 409 ("already exists") for a NOT NULL bug --
// and the caller is an HTTP handler deciding a status code.
func TestIsUniqueViolationTellsAUniqueFailureFromANotNullOne(t *testing.T) {
	conn := openTestDB(t)

	_, err := conn.ExecContext(context.Background(),
		`CREATE TABLE t (email TEXT UNIQUE, name TEXT NOT NULL)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('a@example.com', 'a')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, uniqueErr := conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('a@example.com', 'b')`)
	if uniqueErr == nil {
		t.Fatal("duplicate insert succeeded")
	}

	if !IsUniqueViolation(uniqueErr) {
		t.Errorf("IsUniqueViolation(%v) = false, want true for a duplicate UNIQUE value", uniqueErr)
	}

	_, notNullErr := conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('c@example.com', NULL)`)
	if notNullErr == nil {
		t.Fatal("NULL insert succeeded")
	}

	if IsUniqueViolation(notNullErr) {
		t.Errorf("IsUniqueViolation(%v) = true, want false for a NOT NULL failure", notNullErr)
	}
}

// TestIsUniqueViolationIgnoresTheMessageText is the test that separates a type
// assertion from a string match. A NOT NULL failure on a table whose name happens
// to contain "UNIQUE" produces a message that says UNIQUE without being one, and a
// caller answering 409 on that would be blaming the client for a schema bug.
func TestIsUniqueViolationIgnoresTheMessageText(t *testing.T) {
	conn := openTestDB(t)

	_, err := conn.ExecContext(context.Background(),
		`CREATE TABLE "UNIQUE_THING" (name TEXT NOT NULL)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), `INSERT INTO "UNIQUE_THING" VALUES (NULL)`)
	if err == nil {
		t.Fatal("NULL insert succeeded")
	}

	if IsUniqueViolation(err) {
		t.Errorf("IsUniqueViolation(%v) = true, want false: the message mentions UNIQUE_THING but the failure is NOT NULL", err)
	}
}

// TestIsUniqueViolationRejectsForeignErrors keeps the guard honest: an error that
// is not the driver's at all -- a closed pool, a typo in a table name -- must not
// be reported as a conflict.
func TestIsUniqueViolationRejectsForeignErrors(t *testing.T) {
	if IsUniqueViolation(errors.New("no such table: users")) {
		t.Error("IsUniqueViolation reported a plain error as a unique violation")
	}

	if IsUniqueViolation(nil) {
		t.Error("IsUniqueViolation reported nil as a unique violation")
	}

	_, err := openTestDB(t).ExecContext(context.Background(), `SELECT * FROM missing`)
	if err == nil {
		t.Fatal("query against a missing table succeeded")
	}

	if IsUniqueViolation(err) {
		t.Errorf("IsUniqueViolation(%v) = true, want false for a missing table", err)
	}
}
