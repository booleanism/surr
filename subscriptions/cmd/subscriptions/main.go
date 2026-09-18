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
	"github.com/booleanism/surr/internal/protocols"
	"github.com/booleanism/surr/internal/protocols/rabbitmq"
	"github.com/booleanism/surr/subscriptions/internal/db/sqlite"
	rabbitmqHandlers "github.com/booleanism/surr/subscriptions/internal/protocols/rabbitmq/handlers"
	"github.com/booleanism/surr/subscriptions/internal/usecases"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rabbitmq/amqp091-go"
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

	beforeExit := []func() error{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, f := range beforeExit {
			if err = f(); err != nil {
				fmt.Printf("ERRO: exiting: %s\n", err.Error())
			}
		}
	}()

	switch cmdLine.SelectedCommand {
	case "run":
		run(ctx, cmdLine, beforeExit)
		<-cleanupDone

	default:
	}
}

// nolint:cyclop
func run(ctx context.Context, cmdLine *CommandLine, beforeExit []func() error) {
	ch, routerQueue, err := infraSetup(cmdLine, &beforeExit)
	if err != nil {
		fmt.Printf("ERRO: infra setup: %s\n", err.Error())
		return
	}

	routerHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()
	router := rabbitmq.NewRouter(ch, routerQueue, routerHandlers, siblings)

	sqliteDB, err := sql.Open("sqlite3", cmdLine.DSN)
	if err != nil {
		fmt.Printf("ERRO: opening database: %s\n", err.Error())
		return
	}
	defer sqliteDB.Close() //nolint:errcheck

	txFactory := db.TXFactory(sqliteDB)

	subsRepo := sqlite.NewSubscription(txFactory)
	subsCase := usecases.NewSubscription(subsRepo)
	err = rabbitmqHandlers.SetupSubscription(subsCase, routerHandlers)
	if err != nil {
		fmt.Printf("ERRO: setup rabbitmq subscriptions handler: %s\n", err.Error())
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
				err = router.Introduce(ctx)
				if err != nil {
					fmt.Printf("ERRO: introducing it self: %s\n", err.Error())
				}
			}
		}
	}()

	go func() {
		var restart bool
		fmt.Printf("INFO: starting router listen...\n")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if restart {
					fmt.Printf("INFO: restarting router listen...\n")
				}

				if err = router.Listen(ctx); err != nil {
					fmt.Printf("ERRO: router listen: %s\n", err.Error())
				}

				restart = restart || !restart
			}
		}
	}()

	<-ctx.Done()
}

func infraSetup(cmdLine *CommandLine, beforeExit *[]func() error) (*amqp091.Channel, rabbitmq.QueueSetupReturnFunc, error) {
	mq, err := amqp091.Dial(cmdLine.RabbitMQAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	(*beforeExit) = append((*beforeExit), mq.Close)

	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}

	(*beforeExit) = append((*beforeExit), ch.Close)

	routerQueue, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, fmt.Errorf("setup router queue: %w", err)
	}

	return ch, routerQueue, nil
}
