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
		fmt.Println(err.Error())
		return
	}

	ctx, log := logging.Setup(ctx, cmdLine.SiblingName, cmdLine.FamilyName, cmdLine.LogLevel, cmdLine.LogFormat)

	beforeExit := []func() error{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, f := range beforeExit {
			if err = f(); err != nil {
				log.Error().Err(err).Msg("beforeExit cleanup")
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

func run(ctx context.Context, cmdLine *CommandLine, beforeExit []func() error) {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("ingress", cmdLine.Ingress).
		Str("egress", cmdLine.Egress).
		Uint32("goroutines", cmdLine.NGoroutinesPool).
		Int64("workers", cmdLine.NWorker).
		Msg("rx starting")

	ch, qSetup, err := infraSetup(cmdLine, &beforeExit)
	if err != nil {
		log.Error().Err(err).Msg("setup infra")
		return
	}

	mqHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()
	router := rabbitmq.NewRouter(ch, qSetup, mqHandlers, siblings)

	goPool := pools.NewGoroutinesPool(cmdLine.NGoroutinesPool)
	nodesAdapter := rabbitmqAdapter.NewNodes(router, siblings, goPool)
	subsAdapter := rabbitmqAdapter.NewSubscriptions(router, siblings, goPool)

	options := map[string]string{"mode": "listener"}

	ingressSock := srtgo.NewSrtSocket(cmdLine.IngressHost.Host, cmdLine.IngressHost.Port, options)
	broadcaster := srt.NewBroadcaster(cmdLine.NWorker, *log)
	egressAdrr := fmt.Sprintf("%s:%d", cmdLine.EgressHost.Host, cmdLine.EgressHost.Port)
	ingress := srt.NewIngress(ingressSock, subsAdapter, nodesAdapter, broadcaster, egressAdrr)

	egressSock := srtgo.NewSrtSocket(cmdLine.EgressHost.Host, cmdLine.EgressHost.Port, options)
	egress := srt.NewEgress(egressSock, subsAdapter, broadcaster, *log)

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
					log.Error().Err(err).Msg("introducing it self")
				}
			}
		}
	}()

	go func() {
		listen(ctx, goPool, router, ingress, egress)
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

	qSetup, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, fmt.Errorf("setup router: %w", err)
	}

	return ch, qSetup, nil
}

type service interface {
	Listen(context.Context) error
}

func listen(ctx context.Context, goPool, router, ingress, egress service) {
	log := zerolog.Ctx(ctx)

	go func() {
		goPoolListen(ctx, goPool)
	}()

	go func() {
		routerListen(ctx, router)
	}()

	go func() {
		ingressListen(ctx, ingress)
	}()

	var restart bool
	log.Info().Msg("starting egress listen")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if restart {
				log.Info().Msg("restarting egress listen")
			}

			err := egress.Listen(ctx)
			if err != nil {
				log.Error().Err(err).Msg("egress listen")
			}

			restart = restart || !restart
		}
	}
}

func goPoolListen(ctx context.Context, goPool service) {
	log := zerolog.Ctx(ctx)

	var restart bool
	log.Info().Msg("starting goroutines listen")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if restart {
				log.Info().Msg("restarting goroutines listen")
			}

			err := goPool.Listen(ctx)
			if err != nil {
				log.Error().Err(err).Msg("goroutines listen")
			}

			restart = restart || !restart
		}
	}
}

func routerListen(ctx context.Context, router service) {
	log := zerolog.Ctx(ctx)

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

			err := router.Listen(ctx)
			if err != nil {
				log.Error().Err(err).Msg("router listen")
			}

			restart = restart || !restart
		}
	}
}

func ingressListen(ctx context.Context, ingress service) {
	log := zerolog.Ctx(ctx)

	var restart bool
	log.Info().Msg("starting ingress listen")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if restart {
				log.Info().Msg("restarting ingress listen")
			}

			err := ingress.Listen(ctx)
			if err != nil {
				log.Error().Err(err).Msg("ingress listen")
			}

			restart = restart || !restart
		}
	}
}
