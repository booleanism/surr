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

	// MigrationsPath is where this service's SQL migrations live. It is only
	// used to tell an operator what to run when the database is unmigrated.
	MigrationsPath = "subscriptions/internal/migrations/sqlite"
)

//nolint:cyclop
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmdLine := &CommandLine{
		Version: Version,
	}
	err := cmdLine.parseCLI(ctx, os.Args)
	if err != nil {
		return
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
	case "run":
		err = run(ctx, cmdLine, cleanups)
		if err != nil {
			log.Error().Err(err).Msg("run command")
			cancel()
		}

		<-ctx.Done()
		<-cleanupDone

	default:
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
	defer sqliteDB.Close() //nolint:errcheck

	err = db.VerifySchema(ctx, sqliteDB, MigrationsPath, "subscriptions", "subscribed_nodes")
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
				if introErr := router.Introduce(ctx); introErr != nil {
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
