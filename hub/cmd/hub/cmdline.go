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
	NGoroutinesPool uint32
	LogLevel        string
	LogFormat       string
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
					&cli.StringFlag{
						Name:        "database",
						Aliases:     []string{"d"},
						Value:       "./hub.db",
						Usage:       "hub's sqlite3 dsn",
						Destination: &c.DSN,
						Sources:     cli.EnvVars("DSN"),
					},
					&cli.StringFlag{
						Name:        "secret",
						Aliases:     []string{"s"},
						Usage:       "argon jwt secret",
						Value:       "ac51c5949c74852a16d2ea08884c829aa10425e11097bd954c89c77b3224e7f6",
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
