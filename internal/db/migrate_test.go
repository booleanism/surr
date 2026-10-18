package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func testMigrations() fstest.MapFS {
	return fstest.MapFS{
		"m/000001_first.up.sql":    {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);")},
		"m/000001_first.down.sql":  {Data: []byte("DROP TABLE a;")},
		"m/000002_second.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);")},
		"m/000002_second.down.sql": {Data: []byte("DROP TABLE b;")},
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	database, err := sql.Open(dialectFor().DriverName(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}

	t.Cleanup(func() { database.Close() })

	return database
}

func testMigrator(t *testing.T) Migrator {
	t.Helper()

	up, down, err := LoadMigrations(testMigrations(), "m")
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}

	return Migrator{DB: openTestDB(t), Up: up, Down: down}
}

// mustExist reuses schema.go's tableExists, which asks sqlite_master rather than
// selecting from the table so a name is never interpolated into SQL.
func mustExist(t *testing.T, database *sql.DB, name string) bool {
	t.Helper()

	found, err := tableExists(context.Background(), database, name)
	if err != nil {
		t.Fatalf("reading sqlite_master: %v", err)
	}

	return found
}

// TestApplyIsIdempotent pins the property `migrate` in an entrypoint relies on: a
// second start costs one SELECT, not a second migration.
func TestApplyIsIdempotent(t *testing.T) {
	m := testMigrator(t)

	applied, err := m.Apply(context.Background())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if applied != 2 {
		t.Fatalf("Apply ran %d migrations, want 2", applied)
	}

	applied, err = m.Apply(context.Background())
	if err != nil {
		t.Fatalf("second Apply: %v", err)
	}

	if applied != 0 {
		t.Fatalf("second Apply ran %d migrations, want 0", applied)
	}
}

// TestRevertUndoesExactlyTheLastOne pins the other half: `down` backs out one
// migration, and the version it records afterwards is the highest remaining
// applied one.
func TestRevertUndoesExactlyTheLastOne(t *testing.T) {
	m := testMigrator(t)

	if _, err := m.Apply(context.Background()); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	reverted, err := m.Revert(context.Background())
	if err != nil {
		t.Fatalf("Revert: %v", err)
	}

	if reverted.Version != 2 {
		t.Fatalf("Revert undid version %d, want 2", reverted.Version)
	}

	if mustExist(t, m.DB, "b") {
		t.Error("Revert left table b behind")
	}

	if !mustExist(t, m.DB, "a") {
		t.Error("Revert removed table a, which belongs to version 1")
	}

	// The recorded version, not just the schema: a revert that drops the table but
	// records 0 makes the next `migrate` replay version 1 and fail on a table that
	// already exists.
	status, err := m.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	if status.Version != 1 {
		t.Fatalf("after reverting version 2 the database is at version %d, want 1", status.Version)
	}

	if status.Applied != 1 {
		t.Fatalf("Inspect reports %d applied, want 1", status.Applied)
	}
}

// TestApplyRefusesADirtyDatabase covers the one state this runner cannot produce
// itself: golang-migrate writes `dirty` outside its transaction, so a crash there
// leaves a half-applied schema. Continuing would apply the next migration on top
// of it, so refusing is the only safe answer.
func TestApplyRefusesADirtyDatabase(t *testing.T) {
	m := testMigrator(t)

	conn, err := m.connect(context.Background(), dialectFor())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	defer conn.Close()

	if _, seedErr := conn.ExecContext(context.Background(), "INSERT INTO schema_migrations (version, dirty) VALUES (1, 1)"); seedErr != nil {
		t.Fatalf("seeding a dirty row: %v", seedErr)
	}

	if _, applyErr := m.Apply(context.Background()); !errors.Is(applyErr, ErrDirty) {
		t.Fatalf("Apply on a dirty database returned %v, want ErrDirty", applyErr)
	}
}

// fakeDialect is the seam's own test double: a driver that refuses a
// multi-statement Exec, which SQLite does not, so the split path would otherwise
// never run in this repository.
type fakeDialect struct {
	multi bool
}

func (fakeDialect) DriverName() string { return "sqlite3" }

func (fakeDialect) CreateVersionTable() string {
	return "CREATE TABLE IF NOT EXISTS schema_migrations (version uint64, dirty bool)"
}

func (f fakeDialect) BeginLocking(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")

	return err
}

func (f fakeDialect) MultiStatement() bool { return f.multi }

func (fakeDialect) TransactionalDDL() bool { return true }

func (fakeDialect) BindVar(int) string { return "?" }

// splittingDialect is fakeDialect plus the optional parser. The wrapper exists so
// fakeDialect itself does NOT satisfy statementSplitter -- otherwise the
// "no splitter" branch is unreachable and its test passes for the wrong reason.
type splittingDialect struct {
	splitOf []string
	fakeDialect
}

func (s splittingDialect) SplitStatements(string) []string { return s.splitOf }

// TestExecBodySplitsOnlyWhenTheDialectSaysSo pins both halves of the statement
// decision. One Exec is what SQLite wants and a naive split on ';' would corrupt a
// migration holding a semicolon inside a literal; a driver that refuses
// multi-statement calls must supply its own parser instead.
func TestExecBodySplitsOnlyWhenTheDialectSaysSo(t *testing.T) {
	ctx := context.Background()

	m := testMigrator(t)

	conn, err := m.connect(ctx, dialectFor())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	defer conn.Close()

	// No explicit BEGIN: DDL autocommits, and the tables have to be visible to the
	// pool's other connections for the assertion below to see them.
	d := splittingDialect{splitOf: []string{"CREATE TABLE split_a (id INTEGER)", "CREATE TABLE split_b (id INTEGER)"}}
	if bodyErr := execBody(ctx, conn, d, "ignored"); bodyErr != nil {
		t.Fatalf("execBody: %v", bodyErr)
	}

	if !mustExist(t, m.DB, "split_a") || !mustExist(t, m.DB, "split_b") {
		t.Error("execBody did not run every statement the splitter returned")
	}
}

// TestExecBodyRefusesADriverWithNoSplitter is the failure the runner must name
// rather than paper over: a driver that cannot take a whole file and supplies no
// parser has no safe way to run one.
func TestExecBodyRefusesADriverWithNoSplitter(t *testing.T) {
	ctx := context.Background()

	m := testMigrator(t)

	conn, err := m.connect(ctx, dialectFor())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	defer conn.Close()

	splitErr := execBody(ctx, conn, fakeDialect{}, "SELECT 1")
	if splitErr == nil {
		t.Fatal("execBody accepted a driver with no splitter")
	}

	if !contains(splitErr.Error(), "no splitter") {
		t.Fatalf("execBody returned %q, want it to name the missing splitter", splitErr)
	}
}

// TestSetVersionBindsThroughTheDialect proves the runner does not hardcode "?".
//
// The discriminator cannot be "$1": SQLite reads `$` followed by alphanumerics as a
// NAMED parameter, so it binds happily and the test would pass without proving
// anything. An unparseable token is what makes the failure real.
func TestSetVersionBindsThroughTheDialect(t *testing.T) {
	ctx := context.Background()

	m := testMigrator(t)

	conn, err := m.connect(ctx, dialectFor())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	defer conn.Close()

	if setErr := setVersion(ctx, conn, dialectFor(), 7); setErr != nil {
		t.Fatalf("setVersion with the real dialect: %v", setErr)
	}

	version, dirty, readErr := currentVersion(ctx, conn)
	if readErr != nil {
		t.Fatalf("currentVersion: %v", readErr)
	}

	if version != 7 || dirty {
		t.Fatalf("recorded version %d dirty %v, want 7 false", version, dirty)
	}

	if bindErr := setVersion(ctx, conn, unparseableBindDialect{}, 8); bindErr == nil {
		t.Error("setVersion accepted a nonsense bind variable, so it is not using the dialect's")
	}
}

// unparseableBindDialect renders a placeholder no engine accepts, on purpose: the
// resulting error is the evidence that the runner consults the dialect instead of
// writing "?" itself.
type unparseableBindDialect struct{ fakeDialect }

func (unparseableBindDialect) BindVar(int) string { return "@@bogus" }

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}
