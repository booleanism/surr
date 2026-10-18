package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

// Migration is one migration file: a version, a name, and the SQL to run.
//
// Version comes from the file name, not from its position in the directory, so
// inserting a migration between two others does not silently renumber them.
type Migration struct {
	Name string
	SQL  string
	// Version is the NNNNNN prefix of the file name.
	Version int64
}

// ErrDirty reports a database whose last migration started and did not finish.
//
// It can only be produced by another tool: this runner applies each migration in
// one transaction, so a crash rolls back and leaves the database clean. The check
// exists because golang-migrate, which applied these databases before the
// migrations were embedded, writes `dirty` outside its transaction and can leave
// it set. Continuing from there would apply the next migration over a
// half-applied schema, so refusing is the only safe answer.
var ErrDirty = errors.New("database is dirty")

// ErrNothingToRevert reports `migrate down` on a database that is already empty.
var ErrNothingToRevert = errors.New("no migration to revert")

// Migrator applies embedded migrations to the database the build selected.
//
// The migrations are `//go:embed`ed into each service binary, so a deployment
// needs no golang-migrate install, no checkout, and no build tag: the SQL that
// creates a service's schema ships with the service that uses it.
type Migrator struct {
	DB   *sql.DB
	Up   []Migration
	Down []Migration
}

// Status is what `migrate status` reports.
type Status struct {
	// PendingNames is the name of every migration not yet applied, in order.
	PendingNames []string
	Version      int64
	Applied      int
	Dirty        bool
}

// LoadMigrations reads every NNNNNN_name.up.sql and NNNNNN_name.down.sql under
// dir and returns them in version order.
//
// The naming is golang-migrate's, which is what makes an existing deployment's
// `schema_migrations` table readable here: the version recorded there is the
// number in these file names, so a database migrated by the CLI before the
// migrations were embedded is already at the right version and is not replayed.
func LoadMigrations(fsys fs.FS, dir string) ([]Migration, []Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading migrations in %s: %w", dir, err)
	}

	var up, down []Migration

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		migration, isDown, readErr := readMigration(fsys, dir, entry.Name())
		if readErr != nil {
			return nil, nil, readErr
		}

		if isDown {
			down = append(down, migration)
		} else {
			up = append(up, migration)
		}
	}

	if len(up) == 0 {
		return nil, nil, fmt.Errorf("no .up.sql migrations in %s", dir)
	}

	if versionErr := checkVersions(up, "up"); versionErr != nil {
		return nil, nil, versionErr
	}

	if versionErr := checkVersions(down, "down"); versionErr != nil {
		return nil, nil, versionErr
	}

	sortMigrations(up)
	sortMigrations(down)

	return up, down, nil
}

// readMigration parses one file name and reads its body. It exists so the loop in
// LoadMigrations stays a loop: inlined, the two error paths took that function over
// the repository's cyclomatic-complexity limit.
func readMigration(fsys fs.FS, dir, name string) (Migration, bool, error) {
	migration, isDown, err := parseMigrationName(name)
	if err != nil {
		return Migration{}, false, fmt.Errorf("%s/%s: %w", dir, name, err)
	}

	body, err := fs.ReadFile(fsys, dir+"/"+name)
	if err != nil {
		return Migration{}, false, fmt.Errorf("reading %s/%s: %w", dir, name, err)
	}

	migration.SQL = string(body)

	return migration, isDown, nil
}

// parseMigrationName splits 000002_fleet_scope.up.sql into its parts.
func parseMigrationName(name string) (Migration, bool, error) {
	base := strings.TrimSuffix(name, ".sql")

	stem, direction, ok := strings.Cut(base, ".")
	if !ok {
		return Migration{}, false, fmt.Errorf("name must end in .up.sql or .down.sql")
	}

	var isDown bool

	switch direction {
	case "up":
	case "down":
		isDown = true
	default:
		return Migration{}, false, fmt.Errorf("unknown direction %q (want up or down)", direction)
	}

	prefix, rest, ok := strings.Cut(stem, "_")
	if !ok || rest == "" {
		return Migration{}, false, fmt.Errorf("name must be NNNNNN_name.%s.sql", direction)
	}

	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || version <= 0 {
		return Migration{}, false, fmt.Errorf("version %q is not a positive integer", prefix)
	}

	return Migration{Version: version, Name: rest}, isDown, nil
}

func checkVersions(migrations []Migration, direction string) error {
	seen := make(map[int64]string, len(migrations))

	for _, migration := range migrations {
		if other, dup := seen[migration.Version]; dup {
			return fmt.Errorf(
				"version %d is claimed by both %q and %q (.%s.sql)",
				migration.Version, other, migration.Name, direction,
			)
		}

		seen[migration.Version] = migration.Name
	}

	return nil
}

func sortMigrations(migrations []Migration) {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
}

// Apply runs every pending up migration, oldest first, and reports how many ran.
//
// Applying twice is a no-op: the version recorded in `schema_migrations` is what
// decides, so a service can be deployed with `migrate` in its entrypoint and a
// second start costs one SELECT.
func (m Migrator) Apply(ctx context.Context) (int, error) {
	d := dialectFor()

	conn, err := m.connect(ctx, d)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	current, dirty, versionErr := currentVersion(ctx, conn)
	if versionErr != nil {
		return 0, versionErr
	}

	if dirty {
		return 0, fmt.Errorf("%w at version %d: another tool left it half-applied, inspect it and run `migrate force` or the equivalent before continuing", ErrDirty, current)
	}

	applied := 0

	for _, migration := range m.Up {
		if migration.Version <= current {
			continue
		}

		if applyErr := applyOne(ctx, conn, d, migration); applyErr != nil {
			return applied, fmt.Errorf("migration %06d_%s: %w", migration.Version, migration.Name, applyErr)
		}

		applied++
	}

	return applied, nil
}

// Revert undoes the most recent applied migration and returns it.
//
// One at a time on purpose: `down` that runs to zero is one typo away from
// destroying a production database, and the common need is to back out the last
// change and look at it.
func (m Migrator) Revert(ctx context.Context) (Migration, error) {
	d := dialectFor()

	conn, err := m.connect(ctx, d)
	if err != nil {
		return Migration{}, err
	}
	defer conn.Close()

	current, dirty, err := currentVersion(ctx, conn)
	if err != nil {
		return Migration{}, err
	}

	if dirty {
		return Migration{}, fmt.Errorf("%w at version %d", ErrDirty, current)
	}

	if current == 0 {
		return Migration{}, ErrNothingToRevert
	}

	target, ok := m.downAt(current)
	if !ok {
		return Migration{}, fmt.Errorf(
			"version %d has no .down.sql: reverting it means restoring the database, not running SQL",
			current,
		)
	}

	// The version to record afterwards is the highest remaining applied one, not
	// current-1: versions need not be contiguous, and guessing would skip a
	// migration on the next `up`.
	previous := int64(0)

	for _, migration := range m.Up {
		if migration.Version < current && migration.Version > previous {
			previous = migration.Version
		}
	}

	if revertErr := revertOne(ctx, conn, d, target, previous); revertErr != nil {
		return Migration{}, fmt.Errorf("migration %06d_%s: %w", target.Version, target.Name, revertErr)
	}

	return target, nil
}

// Inspect reports the applied version and what is still pending.
func (m Migrator) Inspect(ctx context.Context) (Status, error) {
	d := dialectFor()

	conn, err := m.connect(ctx, d)
	if err != nil {
		return Status{}, err
	}
	defer conn.Close()

	current, dirty, err := currentVersion(ctx, conn)
	if err != nil {
		return Status{}, err
	}

	status := Status{Version: current, Dirty: dirty, PendingNames: make([]string, 0, len(m.Up))}

	for _, migration := range m.Up {
		if migration.Version <= current {
			status.Applied++

			continue
		}

		status.PendingNames = append(status.PendingNames, fmt.Sprintf("%06d_%s", migration.Version, migration.Name))
	}

	return status, nil
}

func (m Migrator) downAt(version int64) (Migration, bool) {
	for _, migration := range m.Down {
		if migration.Version == version {
			return migration, true
		}
	}

	return Migration{}, false
}

// connect takes one dedicated connection and creates the version table on it.
//
// A single connection rather than the pool because the pragmas below are
// per-connection, and the runner is a one-shot command: there is no concurrency
// to buy by spreading out.
func (m Migrator) connect(ctx context.Context, d dialect) (*sql.Conn, error) {
	conn, err := m.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("taking a connection: %w", err)
	}

	// The version table and the busy-timeout pragma are the dialect's: both are
	// engine syntax, and a wrong one is a runtime error at deploy time rather than
	// a compile error here.
	if _, tableErr := conn.ExecContext(ctx, d.CreateVersionTable()); tableErr != nil {
		conn.Close() //nolint:errcheck // the original error is the one worth reporting
		return nil, fmt.Errorf("creating schema_migrations: %w", tableErr)
	}

	return conn, nil
}

func currentVersion(ctx context.Context, conn *sql.Conn) (int64, bool, error) {
	var (
		version int64
		dirty   bool
	)

	err := conn.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("reading schema_migrations: %w", err)
	}

	return version, dirty, nil
}

// applyOne runs one migration and records its version in the same transaction.
//
// DDL is transactional in SQLite, so a failure halfway through a multi-statement
// file leaves the schema exactly as it was. That is also why `dirty` is only ever
// written as false here: a crash rolls the whole thing back.
func applyOne(ctx context.Context, conn *sql.Conn, d dialect, migration Migration) error {
	return inTransaction(ctx, conn, d, migration.Version, migration.SQL)
}

func revertOne(ctx context.Context, conn *sql.Conn, d dialect, migration Migration, previous int64) error {
	return inTransaction(ctx, conn, d, previous, migration.SQL)
}

func inTransaction(ctx context.Context, conn *sql.Conn, d dialect, version int64, body string) error {
	// The dialect opens the transaction, because how the write lock is taken is
	// engine syntax (SQLite needs BEGIN IMMEDIATE; others do not have the modifier).
	if err := d.BeginLocking(ctx, conn); err != nil {
		return fmt.Errorf("beginning the transaction: %w", err)
	}

	// Rolled back on any error path below. The context is detached because the
	// usual reason to be here is that the context was cancelled.
	committed := false

	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()

	if err := execBody(ctx, conn, d, body); err != nil {
		return err
	}

	if err := setVersion(ctx, conn, d, version); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("committing: %w", err)
	}

	committed = true

	return nil
}

// execBody runs a migration file.
//
// One Exec where the driver accepts several statements in one call -- SQLite does
// -- because a generic split on ';' would corrupt any migration containing a
// semicolon in a string literal, a comment or a trigger body. Where the driver
// refuses multi-statement calls, the dialect supplies its own parser rather than
// the runner guessing.
func execBody(ctx context.Context, conn *sql.Conn, d dialect, body string) error {
	if d.MultiStatement() {
		if _, err := conn.ExecContext(ctx, body); err != nil {
			return fmt.Errorf("executing: %w", err)
		}

		return nil
	}

	splitter, ok := d.(statementSplitter)
	if !ok {
		return fmt.Errorf("driver %q refuses a multi-statement Exec and supplies no splitter", d.DriverName())
	}

	for _, stmt := range splitter.SplitStatements(body) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("executing: %w", err)
		}
	}

	return nil
}

func setVersion(ctx context.Context, conn *sql.Conn, d dialect, version int64) error {
	if _, err := conn.ExecContext(ctx, "DELETE FROM schema_migrations"); err != nil {
		return fmt.Errorf("clearing schema_migrations: %w", err)
	}

	//nolint:gosec // not string interpolation of data: BindVar returns a bind
	// placeholder from a closed set ("?" or "$1"), and the version is still bound.
	query := fmt.Sprintf(
		"INSERT INTO schema_migrations (version, dirty) VALUES (%s, 0)",
		d.BindVar(1),
	)

	if _, err := conn.ExecContext(ctx, query, version); err != nil {
		return fmt.Errorf("recording version %d: %w", version, err)
	}

	return nil
}

// RunMigrate is the body of every service's `migrate` subcommand.
//
// It lives here, not in each cmd/, so the three services that have a schema
// cannot drift in how they apply it: the flags decide the mode, the embedded
// filesystem decides the SQL, and the output is the same shape everywhere.
func RunMigrate(ctx context.Context, dsn, mode string, fsys fs.FS, dir string, out io.Writer) error {
	up, down, err := LoadMigrations(fsys, dir)
	if err != nil {
		return err
	}

	d := dialectFor()

	database, err := sql.Open(d.DriverName(), dsn)
	if err != nil {
		return fmt.Errorf("opening %s: %w", dsn, err)
	}

	defer database.Close()

	if pingErr := database.PingContext(ctx); pingErr != nil {
		return fmt.Errorf("opening %s: %w", dsn, pingErr)
	}

	migrator := Migrator{DB: database, Up: up, Down: down}

	fmt.Fprintf(out, "%s (%s)\n", dsn, mode)

	return runMode(ctx, migrator, mode, out)
}

// runMode is the mode switch, kept out of RunMigrate so neither function has to
// carry the other's branches -- together they were over the complexity limit.
func runMode(ctx context.Context, migrator Migrator, mode string, out io.Writer) error {
	switch mode {
	case "down":
		reverted, err := migrator.Revert(ctx)
		if err != nil {
			return err
		}

		fmt.Fprintf(out, "  reverted %06d_%s\n", reverted.Version, reverted.Name)

		return nil
	case "status":
		return printStatus(ctx, migrator, out)
	default:
		return applyAndReport(ctx, migrator, out)
	}
}

// applyAndReport is `up`: apply, name what this run changed, and print the resulting
// version either way.
//
// Naming the migrations matters in a deploy log: the status listing below says what
// the database contains, not what the command did, and "which migration ran on the
// deploy that broke" is the question a log is read to answer. `down` has always
// named the one it reverted, so this is also the symmetric half.
func applyAndReport(ctx context.Context, migrator Migrator, out io.Writer) error {
	before, err := migrator.Inspect(ctx)
	if err != nil {
		return err
	}

	applied, err := migrator.Apply(ctx)
	if err != nil {
		return err
	}

	if applied == 0 {
		fmt.Fprintln(out, "  already up to date")

		return printStatus(ctx, migrator, out)
	}

	for _, migration := range migrator.Up {
		if migration.Version > before.Version {
			fmt.Fprintf(out, "  applied %06d_%s\n", migration.Version, migration.Name)
		}
	}

	return printStatus(ctx, migrator, out)
}

func printStatus(ctx context.Context, migrator Migrator, out io.Writer) error {
	status, err := migrator.Inspect(ctx)
	if err != nil {
		return err
	}

	for _, migration := range migrator.Up {
		mark := "pending"
		if migration.Version <= status.Version {
			mark = "applied"
		}

		fmt.Fprintf(out, "  %-40s %s\n", fmt.Sprintf("%06d_%s", migration.Version, migration.Name), mark)
	}

	fmt.Fprintf(out, "version %d, %d applied, %d pending\n", status.Version, status.Applied, len(status.PendingNames))

	return nil
}
