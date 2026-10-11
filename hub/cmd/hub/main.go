package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
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
	"github.com/booleanism/surr/internal/restart"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Version is the build id reported by --version and logged at startup. The
// Makefile injects it with -ldflags -X main.Version=$(VERSION), where VERSION
// is `git describe --tags --always --dirty`. A plain `go build` or `go run`
// gets this default instead, which is deliberately not a plausible release
// number.
var Version = "dev"

const (
	IntroInterval  = 1 * time.Second
	GeneralTimeout = 5 * time.Second
	JWTTTL         = 24 * time.Hour
	RefreshTTL     = JWTTTL * 7

	// MaxRequestBodyBytes caps every request body. The largest legitimate
	// body is a subscription add carrying a node list.
	MaxRequestBodyBytes = 1 << 20 // 1 MiB

	// The read and write deadlines are wider than GeneralTimeout because a
	// handler may wait on a broker round trip, but they still bound a
	// slowloris client or a stuck peer.
	HTTPReadTimeout  = 15 * time.Second
	HTTPWriteTimeout = 30 * time.Second
	HTTPIdleTimeout  = 60 * time.Second

	// MigrationsPath is where this service's SQL migrations live. It is only
	// used to tell an operator what to run when the database is unmigrated.
	MigrationsPath = "hub/internal/migrations/sqlite"
)

// resolveJWTSecret returns the configured signing secret, or a random
// per-process one when none is set.
//
// A literal default used to be compiled in, which meant anyone holding the
// repository could mint tokens for any deployment that never set JWT_SECRET.
// Refusing to start would break `go run ./cmd/hub run`, so an unset secret now
// produces an ephemeral key plus a loud warning: tokens stop validating on
// restart, and a second hub instance cannot verify this one's tokens.
func resolveJWTSecret(ctx context.Context, configured string) ([]byte, error) {
	if configured != "" {
		return []byte(configured), nil
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("generating ephemeral jwt secret: %w", err)
	}

	zerolog.Ctx(ctx).Warn().
		Msg("JWT_SECRET is unset: generated an ephemeral signing secret. " +
			"Access tokens will not survive a restart and other hub instances " +
			"cannot validate them; set --secret/JWT_SECRET for any real deployment")

	return buf, nil
}

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

//nolint:cyclop
func run(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

	log.Info().
		Str("broker", logging.RedactURL(cmdLine.RabbitMQAddr)).
		Str("http", cmdLine.HTTPAddr).
		Str("database", cmdLine.DSN).
		Uint32("goroutines", cmdLine.NGoroutinesPool).
		Msg("hub starting")

	sqliteDB, mq, ch, routerQueue, err := infraSetup(cmdLine, cleanups)
	if err != nil {
		return err
	}

	err = db.VerifySchema(ctx, sqliteDB, MigrationsPath, "schema_migrations", "nodes", "users")
	if err != nil {
		return err
	}

	txFactory := db.TXFactory(sqliteDB)

	usersRepo := sqlite.NewUsers(txFactory)
	nodesRepo := sqlite.NewNodes(txFactory)

	routerHandlers := protocols.NewHandlers()
	siblings := protocols.NewSiblings()

	goPool := pools.NewGoroutinesPool(cmdLine.NGoroutinesPool)
	router := rabbitmq.NewRouter(ch, routerQueue, routerHandlers, siblings, goPool)
	supervisor := rabbitmq.NewSupervisor(cmdLine.RabbitMQAddr, cmdLine.SiblingName, cmdLine.FamilyName, router, mq, *log)
	cleanups.Register(supervisor.Close)

	subsAdapter := rabbitmqAdapter.NewSubscription(router, goPool, siblings)
	nodesAdapter := rabbitmqAdapter.NewNodes(router, siblings, goPool)

	argon := auth.NewArgonHashing(auth.ArgonParams{
		Memory:      64 * 1024,
		Iterations:  3,
		Parallelism: 2,
		SaltLength:  16,
		KeyLength:   32,
	})

	subsCase := usecases.NewSubscription(subsAdapter, usersRepo)
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
				if introErr := router.Introduce(ctx, Version); introErr != nil {
					log.Error().Err(introErr).Msg("introducing it self")
				}
			}
		}
	}()

	go supervisor.Watch(ctx)

	go restart.Loop(ctx, "router-listen", router.Listen)
	go restart.Loop(ctx, "goroutines-pool", goPool.Listen)

	refreshToken := auth.NewRefreshTokenStore(RefreshTTL)

	jwtSecret, err := resolveJWTSecret(ctx, cmdLine.JWTSecret)
	if err != nil {
		return err
	}

	auth := auth.NewAuthenticator(usersRepo, argon, jwtSecret, JWTTTL)
	authMiddleware := middlewares.NewAuth(auth)

	subsMux := http.NewServeMux()
	subsHandlers := httpHandlers.NewSubscription(subsMux, subsCase, authMiddleware)

	userMux := http.NewServeMux()
	userHandlers := httpHandlers.NewUser(userMux, userCase)

	nodeMux := http.NewServeMux()
	nodeHandlers := httpHandlers.NewNode(nodeMux, nodeCase, analCase, authMiddleware)

	authMux := http.NewServeMux()
	authHandlers := httpHandlers.NewAuthHandlers(authMux, refreshToken, auth, JWTTTL, cmdLine.CookieSecure)

	apiMux := http.NewServeMux()
	apiMux.Handle("/subscriptions/", http.StripPrefix("/subscriptions", subsHandlers))
	apiMux.Handle("/users/", http.StripPrefix("/users", userHandlers))
	apiMux.Handle("/nodes/", http.StripPrefix("/nodes", nodeHandlers))
	apiMux.Handle("/auth/", http.StripPrefix("/auth", authHandlers))

	mainMux := http.NewServeMux()
	mainMux.Handle("/", apiMux)

	httpSrv := http.Server{
		Addr:              cmdLine.HTTPAddr,
		Handler:           middlewares.MaxBodyBytes(MaxRequestBodyBytes)(middlewares.NewRequestLogger(*log)(mainMux)),
		ReadHeaderTimeout: GeneralTimeout,
		ReadTimeout:       HTTPReadTimeout,
		WriteTimeout:      HTTPWriteTimeout,
		IdleTimeout:       HTTPIdleTimeout,
	}

	cleanups.Register(httpSrv.Close)

	go restart.Loop(ctx, "http-listen", func(ctx context.Context) error {
		serveErr := httpSrv.ListenAndServe()
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}

		return serveErr
	})

	return nil
}

func infraSetup(cmdLine *CommandLine, cleanups *logging.Cleanups) (*sql.DB, *amqp091.Connection, *amqp091.Channel, rabbitmq.QueueSetupReturnFunc, error) {
	sqliteDB, err := sql.Open("sqlite3", cmdLine.DSN)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("opening db: %w", err)
	}
	cleanups.Register(sqliteDB.Close)

	mq, err := amqp091.Dial(cmdLine.RabbitMQAddr)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	cleanups.Register(mq.Close)

	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}

	cleanups.Register(ch.Close)

	routerQueue, err := rabbitmq.SetupRouter(ch, cmdLine.SiblingName, cmdLine.FamilyName)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("setup router queue: %w", err)
	}

	return sqliteDB, mq, ch, routerQueue, nil
}
