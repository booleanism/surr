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
	"github.com/booleanism/surr/internal/protocols"
	"github.com/booleanism/surr/internal/protocols/rabbitmq"
	"github.com/booleanism/surr/subscriptions/internal/db/sqlite"
	rabbitmqHandlers "github.com/booleanism/surr/subscriptions/internal/protocols/rabbitmq/handlers"
	"github.com/booleanism/surr/subscriptions/internal/usecases"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	Version       = "1.0.0"
	IntroInterval = 1 * time.Second
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

	ctx, log := logging.Setup(ctx, cmdLine.SiblingName, cmdLine.FamilyName, cmdLine.LogLevel, cmdLine.LogFormat)

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
		run(ctx, cmdLine, cleanups)
		<-cleanupDone

	default:
	}
}

// nolint:cyclop
func run(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("database", cmdLine.DSN).
		Msg("subscriptions starting")

	ch, routerQueue, err := infraSetup(cmdLine, cleanups)
	if err != nil {
		log.Error().Err(err).Msg("infra setup")
		return
	}

	routerHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()
	router := rabbitmq.NewRouter(ch, routerQueue, routerHandlers, siblings)

	sqliteDB, err := sql.Open("sqlite3", cmdLine.DSN)
	if err != nil {
		log.Error().Err(err).Msg("opening database")
		return
	}
	defer sqliteDB.Close() //nolint:errcheck

	txFactory := db.TXFactory(sqliteDB)

	subsRepo := sqlite.NewSubscription(txFactory)
	subsCase := usecases.NewSubscription(subsRepo)
	err = rabbitmqHandlers.SetupSubscription(subsCase, routerHandlers)
	if err != nil {
		log.Error().Err(err).Msg("setup rabbitmq subscriptions handler")
		return
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

	go func() {
		var restart bool
		log.Info().Msg("starting router listen")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if restart {
					log.Info().Msg("restarting router listen")
				}

				if routerErr := router.Listen(ctx); routerErr != nil {
					log.Error().Err(routerErr).Msg("router listen")
				}

				restart = restart || !restart
			}
		}
	}()

	<-ctx.Done()
}

func infraSetup(cmdLine *CommandLine, cleanups *logging.Cleanups) (*amqp091.Channel, rabbitmq.QueueSetupReturnFunc, error) {
	mq, err := amqp091.Dial(cmdLine.RabbitMQAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	cleanups.Register(mq.Close)

	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}

	cleanups.Register(ch.Close)

	routerQueue, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, fmt.Errorf("setup router queue: %w", err)
	}

	return ch, routerQueue, nil
}
