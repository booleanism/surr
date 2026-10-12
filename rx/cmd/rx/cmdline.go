package main

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"fortio.org/safecast"
	"github.com/urfave/cli/v3"
)

type SRTHost struct {
	Host string
	Port uint16
}

type CommandLine struct {
	SelectedCommand string
	Version         string
	RabbitMQAddr    string
	FamilyName      string
	SiblingName     string
	Ingress         string
	Egress          string
	LogLevel        string
	LogFormat       string
	IngressHost     SRTHost
	EgressHost      SRTHost
	NGoroutinesPool uint32
	NWorker         int64
}

// srtHost parses a listen address of the form <host>:<port>.
//
// net.SplitHostPort rather than a split on ":": the naive version reads
// "[::1]:4200" as host "[" and fails on the port, so an IPv6 listen address
// could not be configured at all. SplitHostPort understands the bracketed form
// and hands back the address without the brackets, which is what srtgo binds.
func (c *CommandLine) srtHost(s string) (SRTHost, error) {
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return SRTHost{}, fmt.Errorf("expected <host>:<port> format: %w", err)
	}

	if host == "" {
		return SRTHost{}, fmt.Errorf("expected <host>:<port> format: empty host")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return SRTHost{}, fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	p, err := safecast.Conv[uint16](port)
	if err != nil {
		return SRTHost{}, err
	}

	return SRTHost{
		Host: host,
		Port: p,
	}, nil
}

func (c *CommandLine) parseSRT() (SRTHost, SRTHost, error) {
	ing, err := c.srtHost(c.Ingress)
	if err != nil {
		return SRTHost{}, SRTHost{}, err
	}

	eg, err := c.srtHost(c.Egress)
	if err != nil {
		return SRTHost{}, SRTHost{}, err
	}

	return ing, eg, nil
}

func (c *CommandLine) parseCLI(ctx context.Context, args []string) error {
	cmd := &cli.Command{
		Name:                  args[0],
		Usage:                 "Receiver Service",
		Authors:               []any{"Mambaul Hisam"},
		Suggest:               true,
		EnableShellCompletion: true,
		Copyright:             "Mambaul Hisam (c) 2026",
		Version:               c.Version,
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "launch receiver service",
				Action: func(ctx context.Context, cc *cli.Command) error {
					c.SelectedCommand = cc.Name
					return nil
				},
				Flags: []cli.Flag{
					&cli.Uint32Flag{
						Name:        "goroutines",
						Aliases:     []string{"g"},
						Usage:       "number of goroutines pool",
						Value:       18,
						Destination: &c.NGoroutinesPool,
					},
					&cli.StringFlag{ //nolint:gosec
						Name:        "broker",
						Aliases:     []string{"b"},
						Usage:       "rabbitmq broker url",
						Value:       "amqp://guest:guest@127.0.0.1:5672//",
						Destination: &c.RabbitMQAddr,
						Sources:     cli.EnvVars("RABBITMQ_URL"),
					},
					&cli.Int64Flag{
						Name:        "worker",
						Aliases:     []string{"w"},
						Usage:       "number of worker. it's determine how many acceptable ingress connections",
						Value:       28,
						Destination: &c.NWorker,
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
						Value:       "rx0",
						Destination: &c.SiblingName,
					},
					&cli.StringFlag{
						Name:        "ingress",
						Aliases:     []string{"i"},
						Value:       "127.0.0.1:4200",
						Usage:       "srt local ingress address",
						Destination: &c.Ingress,
					},
					&cli.StringFlag{
						Name:        "egress",
						Aliases:     []string{"e"},
						Value:       "127.0.0.1:4201",
						Usage:       "srt local egress address",
						Destination: &c.Egress,
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
			},
		},
		Before: func(ctx context.Context, cc *cli.Command) (context.Context, error) {
			ing, eg, err := c.parseSRT()
			if err != nil {
				return ctx, err
			}

			c.IngressHost = ing
			c.EgressHost = eg
			return ctx, nil
		},
	}
	err := cmd.Run(ctx, args)
	if err != nil {
		return err
	}

	return nil
}
