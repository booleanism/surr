package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	rabbitmqAdapter "github.com/booleanism/surr/hub/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/hub/internal/db/sqlite"
	httpHandlers "github.com/booleanism/surr/hub/internal/protocols/http/handlers"
	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	rabbitmqHandlers "github.com/booleanism/surr/hub/internal/protocols/rabbitmq/handlers"
	"github.com/booleanism/surr/hub/internal/usecases"
	"github.com/booleanism/surr/hub/internal/usecases/auth"
	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/logging"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/booleanism/surr/internal/protocols/rabbitmq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	IntroInterval  = 1 * time.Second
	Version        = "1.0.0"
	GeneralTimeout = 5 * time.Second
	JWTTTL         = 24 * time.Hour
	RefreshTTL     = JWTTTL * 7
)

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

//nolint:cyclop
func run(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("http", cmdLine.HTTPAddr).
		Str("database", cmdLine.DSN).
		Uint32("goroutines", cmdLine.NGoroutinesPool).
		Msg("hub starting")

	sqliteDB, ch, routerQueue, err := infraSetup(cmdLine, cleanups)
	if err != nil {
		return err
	}

	txFactory := db.TXFactory(sqliteDB)

	usersRepo := sqlite.NewUsers(txFactory)
	nodesRepo := sqlite.NewNodes(txFactory)

	routerHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()

	router := rabbitmq.NewRouter(ch, routerQueue, routerHandlers, siblings)
	goPool := pools.NewGoroutinesPool(cmdLine.NGoroutinesPool)
	subsAdapter := rabbitmqAdapter.NewSubscription(router, goPool, siblings)
	nodesAdapter := rabbitmqAdapter.NewNodes(router, siblings, goPool)

	argon := auth.NewArgonHashing(auth.ArgonParams{
		Memory:      64 * 1024,
		Iterations:  3,
		Parallelism: 2,
		SaltLength:  16,
		KeyLength:   32,
	})

	subsCase := usecases.NewSubscription(subsAdapter, usersRepo, nodesRepo)
	userCase := usecases.NewUser(usersRepo, argon)
	nodeCase := usecases.NewNode(nodesRepo, subsAdapter)
	analCase := usecases.NewAnalytics(subsAdapter, nodesAdapter)

	err = rabbitmqHandlers.SetupNode(nodeCase, routerHandlers)
	if err != nil {
		return fmt.Errorf("setup rabbitmq node handler: %w", err)
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

	go func() {
		var restart bool
		log.Info().Msg("starting goroutines pool")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if restart {
					log.Info().Msg("restarting goroutine pool")
				}

				if poolErr := goPool.Listen(ctx); poolErr != nil {
					log.Error().Err(poolErr).Msg("goroutines pool listen")
				}

				restart = restart || !restart
			}
		}
	}()

	refreshToken := auth.NewRefreshTokenStore(RefreshTTL)
	auth := auth.NewAuthenticator(usersRepo, argon, []byte(cmdLine.JWTSecret), JWTTTL)
	authMiddleware := middlewares.NewAuth(auth)
	_ = authMiddleware

	subsMux := http.NewServeMux()
	subsHandlers := httpHandlers.NewSubscription(subsMux, subsCase, authMiddleware)

	userMux := http.NewServeMux()
	userHandlers := httpHandlers.NewUser(userMux, userCase)

	nodeMux := http.NewServeMux()
	nodeHandlers := httpHandlers.NewNode(nodeMux, nodeCase, analCase, authMiddleware)

	authMux := http.NewServeMux()
	authHandlers := httpHandlers.NewAuthHandlers(authMux, refreshToken, auth, JWTTTL)

	apiMux := http.NewServeMux()
	apiMux.Handle("/subscriptions/", http.StripPrefix("/subscriptions", subsHandlers))
	apiMux.Handle("/users/", http.StripPrefix("/users", userHandlers))
	apiMux.Handle("/nodes/", http.StripPrefix("/nodes", nodeHandlers))
	apiMux.Handle("/auth/", http.StripPrefix("/auth", authHandlers))

	mainMux := http.NewServeMux()
	mainMux.Handle("/", apiMux)

	httpSrv := http.Server{
		Addr:              cmdLine.HTTPAddr,
		Handler:           middlewares.NewRequestLogger(*log)(mainMux),
		ReadHeaderTimeout: GeneralTimeout,
	}

	cleanups.Register(httpSrv.Close)

	go func() {
		var restart bool
		log.Info().Str("addr", cmdLine.HTTPAddr).Msg("starting http listen")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if restart {
					log.Info().Str("addr", cmdLine.HTTPAddr).Msg("restarting http listen")
				}

				if httpErr := httpSrv.ListenAndServe(); httpErr != nil {
					log.Error().Err(httpErr).Msg("http listen")
				}

				restart = restart || !restart
			}
		}
	}()

	return nil
}

func infraSetup(cmdLine *CommandLine, cleanups *logging.Cleanups) (*sql.DB, *amqp091.Channel, rabbitmq.QueueSetupReturnFunc, error) {
	sqliteDB, err := sql.Open("sqlite3", cmdLine.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opening db: %w", err)
	}
	cleanups.Register(sqliteDB.Close)

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

	return sqliteDB, ch, routerQueue, nil
}
