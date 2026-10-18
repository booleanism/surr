//go:build !postgres

// Package migrations embeds this service's SQL migrations into its binary.
//
// Embedding is what makes `migrate` a subcommand of the service rather than a
// separate tool: a deployment ships one artifact, and the SQL that creates the
// schema it needs travels inside it. It also removes the failure this replaces --
// a golang-migrate release binary has no SQLite driver unless it is built with
// `-tags sqlite3`, so "the migrator cannot open the database" was a build-tag
// problem for every operator to hit once.
package migrations

import "embed"

// FS holds the migration files; Dir is the path to them inside it. Load them with
// db.LoadMigrations(FS, Dir).
//
// The build tag on this file is the engine selector, and it has to match
// internal/db's dialect: the SQL is engine syntax -- BLOB, randomblob(16), PRAGMA
// -- so the files that get embedded are chosen by the same tag. Embedding sqlite/
// unconditionally would let a Postgres binary ship SQLite SQL, which fails at
// deploy time in a way no compiler notices. With the tag, a `-tags postgres` build
// fails here, naming exactly what is missing.
//
//go:embed sqlite/*.sql
var FS embed.FS

// Dir is the directory inside FS that holds the .sql files.
const Dir = "sqlite"
