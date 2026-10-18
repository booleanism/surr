package main

import (
	"context"
	"testing"
)

// parse is a helper that parses args as if they were the process command line,
// which is the only way to exercise the flag wiring urfave/cli builds.
func parse(t *testing.T, args ...string) CommandLine {
	t.Helper()

	c := &CommandLine{}

	if err := c.parseCLI(context.Background(), append([]string{"surr-hub"}, args...)); err != nil {
		t.Fatalf("parseCLI(%v): %v", args, err)
	}

	return *c
}

// TestMigrateAcceptsTheDatabaseFlagInEitherPosition pins the defect that made
// `migrate -d /data/hub.db up` migrate ./hub.db instead.
//
// The cause was a Destination on the subcommand's flag: urfave/cli applies a
// flag's DEFAULT to its Destination even when the flag was not given, so the
// subcommand's "./hub.db" overwrote the value the parent had already parsed. The
// failure is silent -- a fresh database in the working directory, no error -- and
// in a deploy script it means the service starts against a schema nobody
// migrated.
func TestMigrateAcceptsTheDatabaseFlagInEitherPosition(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "before the mode", args: []string{"migrate", "-d", "/data/hub.db", "up"}},
		{name: "after the mode", args: []string{"migrate", "up", "-d", "/data/hub.db"}},
		{name: "long form before", args: []string{"migrate", "--database", "/data/hub.db", "up"}},
		{name: "long form after", args: []string{"migrate", "status", "--database", "/data/hub.db"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := parse(t, tc.args...)

			if c.SelectedCommand != "migrate" {
				t.Fatalf("SelectedCommand is %q, want migrate", c.SelectedCommand)
			}

			if c.DSN != "/data/hub.db" {
				t.Fatalf("DSN is %q, want /data/hub.db", c.DSN)
			}
		})
	}
}

// TestMigrateDefaultsTheDatabase covers the bare forms: the default has to come
// from the parent command, which is the one place it is declared.
func TestMigrateDefaultsTheDatabase(t *testing.T) {
	for _, args := range [][]string{{"migrate"}, {"migrate", "up"}, {"migrate", "down"}, {"migrate", "status"}} {
		c := parse(t, args...)

		if c.SelectedCommand != "migrate" {
			t.Fatalf("%v: SelectedCommand is %q, want migrate", args, c.SelectedCommand)
		}

		if c.DSN != "./hub.db" {
			t.Fatalf("%v: DSN is %q, want the default ./hub.db", args, c.DSN)
		}
	}
}

// TestMigrateModeIsTheLeafCommand pins the mode, because `migrate` with no
// subcommand must mean `up` -- that is what an operator types at a fresh database
// -- while the leaves must not all collapse to it.
func TestMigrateModeIsTheLeafCommand(t *testing.T) {
	for _, tc := range []struct {
		want string
		args []string
	}{
		{args: []string{"migrate"}, want: "up"},
		{args: []string{"migrate", "up"}, want: "up"},
		{args: []string{"migrate", "down"}, want: "down"},
		{args: []string{"migrate", "status"}, want: "status"},
	} {
		if got := parse(t, tc.args...).MigrateMode; got != tc.want {
			t.Fatalf("%v: MigrateMode is %q, want %q", tc.args, got, tc.want)
		}
	}
}

// TestMigrateTakesTheEnvironmentSource proves the default is resolved in one
// place: the parent command owns both the literal default and the env var, so the
// subcommands cannot disagree with it.
func TestMigrateTakesTheEnvironmentSource(t *testing.T) {
	t.Setenv("DSN", "/data/from-env.db")

	for _, args := range [][]string{{"migrate", "up"}, {"migrate", "-d", "/data/flag.db", "up"}} {
		c := parse(t, args...)

		want := "/data/from-env.db"
		if len(args) == 4 {
			want = "/data/flag.db"
		}

		if c.DSN != want {
			t.Fatalf("%v: DSN is %q, want %q", args, c.DSN, want)
		}
	}
}
