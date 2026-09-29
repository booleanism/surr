package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"fortio.org/safecast"
	"github.com/booleanism/surr/tx/internal/core"
	"github.com/urfave/cli/v3"
)

type ServeCmdLine struct{}

type RunCmdLine struct {
	Sink            string
	Source          string
	StreamName      string
	DownstreamSpecs core.StreamSpecs
	RecordingSpecs  core.StreamSpecs
}

type CommandLine struct {
	Version         string
	RecordingDir    string
	SelectedCommand string
	LogLevel        string
	LogFormat       string
	Run             RunCmdLine
	Serve           ServeCmdLine
	DashPort        uint16
}

func (cl CommandLine) parseSpecs(s string) (uint64, uint64, uint32, error) {
	ss := strings.Split(s, ",")
	if len(ss) < 3 {
		return 0, 0, 0, fmt.Errorf("unexpected format")
	}

	wint, err := strconv.Atoi(ss[0])
	if err != nil {
		return 0, 0, 0, err
	}

	hint, err := strconv.Atoi(ss[1])
	if err != nil {
		return 0, 0, 0, err
	}

	bint, err := strconv.Atoi(ss[2])
	if err != nil {
		return 0, 0, 0, err
	}

	w, err := safecast.Conv[uint64](wint)
	if err != nil {
		return 0, 0, 0, err
	}

	h, err := safecast.Conv[uint64](hint)
	if err != nil {
		return 0, 0, 0, err
	}

	b, err := safecast.Conv[uint32](bint)
	if err != nil {
		return 0, 0, 0, err
	}

	return w, h, b, nil
}

func (cl *CommandLine) parseCLI(ctx context.Context, args []string) error {
	cmd := &cli.Command{
		Name:                  args[0],
		Usage:                 "Transmitter Service",
		Authors:               []any{"Mambaul Hisam"},
		Suggest:               true,
		EnableShellCompletion: true,
		Copyright:             "Mambaul Hisam (c) 2026",
		Version:               cl.Version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dir",
				Aliases:     []string{"d"},
				Usage:       "recording directory",
				Value:       os.TempDir(),
				Destination: &cl.RecordingDir,
			},
			&cli.Uint16Flag{
				Name:        "port",
				Aliases:     []string{"p"},
				Usage:       "local dash server port",
				Value:       8000,
				Destination: &cl.DashPort,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "log level (debug|info|warn|error)",
				Value:       "info",
				Destination: &cl.LogLevel,
			},
			&cli.StringFlag{
				Name:        "log-format",
				Usage:       "log output format (console|json)",
				Value:       "console",
				Destination: &cl.LogFormat,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "start dash streaming server",
				Action: func(ctx context.Context, c *cli.Command) error {
					cl.SelectedCommand = c.Name
					return nil
				},
			},
			{
				Name:  "run",
				Usage: "transmit video and/or audio",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "src",
						Aliases:     []string{"s"},
						Usage:       "rtsp source upstream/cctv url (example: rtsp://127.0.0.1:8554/cam1)",
						Destination: &cl.Run.Source,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "sink",
						Aliases:     []string{"S"},
						Usage:       "sink address. only support srt, formatted as srt://127.0.0.1:4200?mode=caller&streamid=publish:<your subscription id>",
						Required:    true,
						Destination: &cl.Run.Sink,
					},
					&cli.StringFlag{
						Name:        "name",
						Aliases:     []string{"n"},
						Usage:       "named stream",
						Required:    true,
						Destination: &cl.Run.StreamName,
					},
					&cli.StringFlag{
						Name:    "downstream",
						Aliases: []string{"d"},
						Usage:   "downstream specs. separated by comma: <width>,<height>,<bitrate>",
						Value:   "1280,720,300",
						Action: func(ctx context.Context, cc *cli.Command, s string) error {
							w, h, b, err := cl.parseSpecs(s)
							if err != nil {
								return err
							}

							cl.Run.DownstreamSpecs.Width = w
							cl.Run.DownstreamSpecs.Height = h
							cl.Run.DownstreamSpecs.Bitrate = b
							return nil
						},
					},
					&cli.StringFlag{
						Name:    "recording",
						Aliases: []string{"r"},
						Usage:   "recording specs. same format as --downstrean",
						Value:   "0,0,0",
						Action: func(ctx context.Context, cc *cli.Command, s string) error {
							w, h, b, err := cl.parseSpecs(s)
							if err != nil {
								return err
							}

							cl.Run.RecordingSpecs.Width = w
							cl.Run.RecordingSpecs.Height = h
							cl.Run.RecordingSpecs.Bitrate = b
							return nil
						},
					},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					cl.SelectedCommand = c.Name
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
