package main

import (
	"context"

	"github.com/urfave/cli/v3"
)

type CommandLine struct {
	DSN             string
	Version         string
	SiblingName     string
	FamilyName      string
	HTTPAddr        string
	JWTSecret       string
	RabbitMQAddr    string
	SelectedCommand string
	MigrateMode     string
	LogLevel        string
	LogFormat       string
	NGoroutinesPool uint32
	CookieSecure    bool
}

func (c *CommandLine) parseCLI(ctx context.Context, args []string) error {
	cmd := &cli.Command{
		Name:                  args[0],
		Usage:                 "Hub Service",
		Authors:               []any{"Mambaul Hisam"},
		EnableShellCompletion: true,
		Suggest:               true,
		Copyright:             "Mambaul Hisam (c) 2026",
		Version:               c.Version,
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "launch hub service",
				Flags: []cli.Flag{
					&cli.Uint32Flag{
						Name:        "goroutines",
						Aliases:     []string{"g"},
						Usage:       "number of goroutines pool",
						Value:       18,
						Destination: &c.NGoroutinesPool,
					},
					databaseFlag(&c.DSN),
					&cli.StringFlag{
						Name:        "secret",
						Aliases:     []string{"s"},
						Usage:       "jwt signing secret. required in production: without it a random per-process secret is generated and tokens stop being valid after a restart",
						Destination: &c.JWTSecret,
						Sources:     cli.EnvVars("JWT_SECRET"),
					},
					&cli.StringFlag{ //nolint:gosec
						Name:        "broker",
						Aliases:     []string{"b"},
						Usage:       "rabbitmq broker url",
						Value:       "amqp://guest:guest@127.0.0.1:5672//",
						Destination: &c.RabbitMQAddr,
						Sources:     cli.EnvVars("RABBITMQ_URL"),
					},
					&cli.StringFlag{
						Name:        "family",
						Aliases:     []string{"f"},
						Usage:       "broker family name",
						Value:       "surr0",
						Destination: &c.FamilyName,
					},
					&cli.StringFlag{
						Name:        "name",
						Aliases:     []string{"n"},
						Usage:       "service name. it's should unique each deployment instance",
						Value:       "hub0",
						Destination: &c.SiblingName,
					},
					&cli.StringFlag{
						Name:        "http",
						Aliases:     []string{"a"},
						Usage:       "local http listen",
						Value:       "127.0.0.1:9000",
						Destination: &c.HTTPAddr,
					},
					&cli.BoolFlag{
						Name:        "cookie-secure",
						Usage:       "set the Secure attribute on the refresh cookie. disable only for a plain-HTTP local deployment, where the browser would otherwise drop it",
						Value:       true,
						Destination: &c.CookieSecure,
					},
					&cli.StringFlag{
						Name:        "log-level",
						Usage:       "log level (debug|info|warn|error)",
						Value:       "info",
						Destination: &c.LogLevel,
					},
					&cli.StringFlag{
						Name:        "log-format",
						Usage:       "log output format (console|json)",
						Value:       "console",
						Destination: &c.LogFormat,
					},
				},
				Action: func(ctx context.Context, cc *cli.Command) error {
					c.SelectedCommand = cc.Name
					return nil
				},
			},
			{
				Name:  "migrate",
				Usage: "apply the SQL migrations embedded in this binary",
				Flags: []cli.Flag{databaseFlag(&c.DSN)},
				Commands: []*cli.Command{
					{
						Name:   "up",
						Usage:  "apply every pending migration",
						Flags:  []cli.Flag{migrateFlag()},
						Action: c.migrateAction("up"),
					},
					{
						Name:   "down",
						Usage:  "revert the most recent migration, one at a time",
						Flags:  []cli.Flag{migrateFlag()},
						Action: c.migrateAction("down"),
					},
					{
						Name:   "status",
						Usage:  "report the applied version and what is still pending",
						Flags:  []cli.Flag{migrateFlag()},
						Action: c.migrateAction("status"),
					},
				},
				// `surr-x migrate` with no subcommand is `up`, because that is what
				// an operator types at a fresh database.
				Action: c.migrateAction("up"),
			},
		},
	}

	if err := cmd.Run(ctx, args); err != nil {
		return err
	}

	return nil
}

// databaseFlag is declared once and used by both `run` and `migrate`: two copies
// would drift, and a migration that opened a different file than the service
// reads is a failure nobody sees until the first query.
func databaseFlag(dsn *string) cli.Flag {
	return &cli.StringFlag{
		Name:        "database",
		Aliases:     []string{"d"},
		Value:       "./hub.db",
		Usage:       "hub's sqlite3 dsn",
		Destination: dsn,
		Sources:     cli.EnvVars("DSN"),
	}
}

// migrateFlag lets `--database` be written after the mode as well as before it.
//
// It has no Destination on purpose: see migrateAction. The parent `migrate` command
// owns the default and the environment source, so there is exactly one place a
// missing value is resolved.
func migrateFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "database",
		Aliases: []string{"d"},
		Usage:   "sqlite3 dsn (also accepted before the mode)",
	}
}

// migrateAction only records which migration was asked for; main runs it, so a
// failed migration can exit non-zero and a deploy script can tell.
func (c *CommandLine) migrateAction(mode string) cli.ActionFunc {
	return func(_ context.Context, cc *cli.Command) error {
		c.SelectedCommand = "migrate"
		c.MigrateMode = mode

		// Only when the subcommand was actually given the flag. Reading it
		// unconditionally would not help and is not what fixes this: the defect was
		// a Destination on the subcommand, which applied the flag's DEFAULT over the
		// value the parent had already parsed, so `migrate -d /data/hub.db up`
		// silently migrated ./hub.db instead.
		if cc.IsSet("database") {
			c.DSN = cc.String("database")
		}

		return nil
	}
}
