package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/booleanism/surr/internal/logging"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/booleanism/surr/internal/protocols/rabbitmq"
	"github.com/booleanism/surr/internal/restart"
	rabbitmqAdapter "github.com/booleanism/surr/rx/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/rx/internal/protocols/srt"
	"github.com/haivision/srtgo"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	Version       = "1.0.0"
	IntroInterval = 1 * time.Second
)

func main() {
	pools.SetBytesPoolSize(4096)
	srtgo.InitSRT()
	srtgo.SrtSetLogLevel(srtgo.SrtLogLevelCrit)
	defer srtgo.CleanupSRT()

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

func run(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("ingress", cmdLine.Ingress).
		Str("egress", cmdLine.Egress).
		Uint32("goroutines", cmdLine.NGoroutinesPool).
		Int64("workers", cmdLine.NWorker).
		Msg("rx starting")

	mq, ch, qSetup, err := infraSetup(cmdLine, cleanups)
	if err != nil {
		return err
	}

	mqHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()
	goPool := pools.NewGoroutinesPool(cmdLine.NGoroutinesPool)
	router := rabbitmq.NewRouter(ch, qSetup, mqHandlers, siblings, goPool)
	supervisor := rabbitmq.NewSupervisor(cmdLine.RabbitMQAddr, cmdLine.SiblingName, cmdLine.FamilyName, router, mq, *log)
	cleanups.Register(supervisor.Close)

	nodesAdapter := rabbitmqAdapter.NewNodes(router, siblings, goPool)
	subsAdapter := rabbitmqAdapter.NewSubscriptions(router, siblings, goPool)

	options := map[string]string{"mode": "listener"}

	ingressSock := srtgo.NewSrtSocket(cmdLine.IngressHost.Host, cmdLine.IngressHost.Port, options)
	cleanups.Register(func() error {
		ingressSock.Close()
		return nil
	})

	broadcaster := srt.NewBroadcaster(cmdLine.NWorker, *log)
	egressAdrr := fmt.Sprintf("%s:%d", cmdLine.EgressHost.Host, cmdLine.EgressHost.Port)
	ingress := srt.NewIngress(ingressSock, subsAdapter, nodesAdapter, broadcaster, egressAdrr)

	egressSock := srtgo.NewSrtSocket(cmdLine.EgressHost.Host, cmdLine.EgressHost.Port, options)
	cleanups.Register(func() error {
		egressSock.Close()
		return nil
	})

	egress := srt.NewEgress(egressSock, subsAdapter, broadcaster, *log)

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

	go restart.Loop(ctx, "go-pool", goPool.Listen)
	go restart.Loop(ctx, "router", router.Listen)
	go restart.Loop(ctx, "ingress", ingress.Listen)
	go restart.Loop(ctx, "egress", egress.Listen)

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

	qSetup, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("setup router: %w", err)
	}

	return mq, ch, qSetup, nil
}
