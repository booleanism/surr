package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/logging"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/booleanism/surr/internal/protocols/rabbitmq"
	"github.com/booleanism/surr/internal/restart"
	"github.com/booleanism/surr/subscriptions/internal/db/sqlite"
	"github.com/booleanism/surr/subscriptions/internal/migrations"
	rabbitmqHandlers "github.com/booleanism/surr/subscriptions/internal/protocols/rabbitmq/handlers"
	"github.com/booleanism/surr/subscriptions/internal/usecases"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Version is the build id reported by --version and logged at startup. The
// Makefile injects it with -ldflags -X main.Version=$(VERSION); see the
// Makefile for how VERSION is derived.
var Version = "dev"

const (
	IntroInterval = 1 * time.Second
)

//nolint:cyclop
func main() {
	os.Exit(mainEntry())
}

// mainEntry is main's body, returning the process exit code rather than calling
// os.Exit itself. os.Exit skips deferred calls, and the deferred cancellation here
// is what lets the cleanup hooks run at all -- which is the shape tx already uses,
// for the same reason.
func mainEntry() int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmdLine := &CommandLine{
		Version: Version,
	}

	if err := cmdLine.parseCLI(ctx, os.Args); err != nil {
		return 1
	}

	ctx, log := logging.Setup(ctx, cmdLine.SiblingName, cmdLine.FamilyName, Version, cmdLine.LogLevel, cmdLine.LogFormat)

	cleanups := &logging.Cleanups{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, cleanupErr := range cleanups.Run() {
			log.Error().Err(cleanupErr).Msg("beforeExit cleanup")
		}
	}()

	switch cmdLine.SelectedCommand {
	case "migrate":
		// Exits non-zero, unlike the run path: a deploy script that carries on
		// after a failed migration starts a service against the wrong schema, and
		// the service's own error says only which table is missing.
		if migrateErr := db.RunMigrate(ctx, cmdLine.DSN, cmdLine.MigrateMode, migrations.FS, migrations.Dir, os.Stdout); migrateErr != nil {
			log.Error().Err(migrateErr).Msg("migrate command")
			logging.DrainCleanups(cancel, cleanupDone)

			return 1
		}

		logging.DrainCleanups(cancel, cleanupDone)

		return 0
	case "run":
		runErr := run(ctx, cmdLine, cleanups)
		if runErr != nil {
			log.Error().Err(runErr).Msg("run command")
			cancel()
		}

		<-ctx.Done()
		logging.DrainCleanups(cancel, cleanupDone)

		if runErr != nil {
			return 1
		}

		return 0
	default:
		return 0
	}
}

// nolint:cyclop
func run(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("database", cmdLine.DSN).
		Msg("subscriptions starting")

	mq, ch, routerQueue, err := infraSetup(cmdLine, cleanups)
	if err != nil {
		return err
	}

	routerHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()
	goPool := pools.NewGoroutinesPool(cmdLine.NGoroutinesPool)
	router := rabbitmq.NewRouter(ch, routerQueue, routerHandlers, siblings, goPool)
	supervisor := rabbitmq.NewSupervisor(cmdLine.RabbitMQAddr, cmdLine.SiblingName, cmdLine.FamilyName, router, mq, *log)
	cleanups.Register(supervisor.Close)

	sqliteDB, err := sql.Open("sqlite3", cmdLine.DSN)
	if err != nil {
		return err
	}

	// Registered, not deferred. run() does not block -- it starts the listener
	// goroutines and returns -- so a deferred close would shut the database
	// immediately after startup, and every command after that would fail with
	// "sql: database is closed". The hub registers its own handle the same way
	// (see cleanups.Register(sqliteDB.Close) in hub/cmd/hub/main.go).
	cleanups.Register(sqliteDB.Close)

	err = db.VerifySchema(ctx, sqliteDB,
		"surr-subscriptions migrate -d "+cmdLine.DSN, "subscriptions", "subscribed_nodes")
	if err != nil {
		return err
	}

	txFactory := db.TXFactory(sqliteDB)

	subsRepo := sqlite.NewSubscription(txFactory)
	subsCase := usecases.NewSubscription(subsRepo)
	err = rabbitmqHandlers.SetupSubscription(subsCase, routerHandlers)
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(IntroInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if introErr := router.Introduce(ctx, Version); introErr != nil {
					log.Error().Err(introErr).Msg("introducing it self")
				}
			}
		}
	}()

	go supervisor.Watch(ctx)

	go restart.Loop(ctx, "router-listen", router.Listen)
	go restart.Loop(ctx, "goroutines-pool", goPool.Listen)

	return nil
}

func infraSetup(cmdLine *CommandLine, cleanups *logging.Cleanups) (*amqp091.Connection, *amqp091.Channel, rabbitmq.QueueSetupReturnFunc, error) {
	mq, err := amqp091.Dial(cmdLine.RabbitMQAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	cleanups.Register(mq.Close)

	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}

	cleanups.Register(ch.Close)

	routerQueue, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("setup router queue: %w", err)
	}

	return mq, ch, routerQueue, nil
}
