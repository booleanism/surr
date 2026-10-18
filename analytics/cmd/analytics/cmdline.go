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
	RabbitMQAddr    string
	MongoURL        string
	MongoDatabase   string
	SelectedCommand string
	LogLevel        string
	LogFormat       string
	NGoroutinesPool uint32
}

func (c *CommandLine) parseCLI(ctx context.Context, args []string) error {
	cmd := &cli.Command{
		Name:                  args[0],
		Usage:                 "Analytics Service",
		Authors:               []any{"Mambaul Hisam"},
		Suggest:               true,
		EnableShellCompletion: true,
		Copyright:             "Mambaul Hisam (c) 2026",
		Version:               c.Version,
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "launch analytics service",
				Flags: []cli.Flag{
					&cli.Uint32Flag{
						Name:        "goroutines",
						Aliases:     []string{"g"},
						Usage:       "number of goroutines pool",
						Value:       18,
						Destination: &c.NGoroutinesPool,
					},
					&cli.StringFlag{
						Name:        "database",
						Aliases:     []string{"d"},
						Value:       "./analytics.db",
						Usage:       "analytics' sqlite3 dsn holding the declared query manifests",
						Destination: &c.DSN,
						Sources:     cli.EnvVars("DSN"),
					},
					&cli.StringFlag{ //nolint:gosec
						Name:        "broker",
						Aliases:     []string{"b"},
						Usage:       "rabbitmq broker url",
						Value:       "amqp://guest:guest@127.0.0.1:5672//",
						Destination: &c.RabbitMQAddr,
						Sources:     cli.EnvVars("RABBITMQ_URL"),
					},
					&cli.StringFlag{ //nolint:gosec
						Name:        "mongo-url",
						Usage:       "mongodb url the declared queries read from",
						Value:       "mongodb://127.0.0.1:27017/",
						Destination: &c.MongoURL,
						Sources:     cli.EnvVars("MONGO_URL"),
					},
					&cli.StringFlag{
						Name:        "mongo-db",
						Usage:       "mongodb database holding the analytics documents",
						Value:       "nodes",
						Destination: &c.MongoDatabase,
						Sources:     cli.EnvVars("MONGO_DB"),
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
						Value:       "analytics0",
						Destination: &c.SiblingName,
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
		},
	}

	if err := cmd.Run(ctx, args); err != nil {
		return err
	}

	return nil
}
