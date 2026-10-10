package rabbitmq

import (
	"context"
	"sync"

	"github.com/booleanism/surr/internal/backoff"
	"github.com/booleanism/surr/internal/logging"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Supervisor keeps a service's broker connection alive.
//
// A service publishes and consumes on the channel the router holds. When the
// broker closes that channel — a broker restart, a network blip, a missed
// heartbeat — every call on it fails immediately, so the service stays up but
// deaf until somebody restarts it. The supervisor watches the connection,
// dials again with exponential backoff, declares a fresh queue and installs it
// with router.ReplaceChannel; the service's restart loop then re-enters
// router.Listen against the new channel.
//
// The initial connection is established by the service before the supervisor
// exists (the router cannot be built without a channel), so it is passed in.
type Supervisor struct {
	addr     string
	name     string
	exchange string

	router *router
	log    zerolog.Logger

	mu   sync.Mutex
	conn *amqp091.Connection
}

// NewSupervisor returns a supervisor for router. addr is the broker URI, name
// the service's sibling name and exchange the family exchange, all of which are
// needed to declare an equivalent queue on a new connection.
func NewSupervisor(addr, name, exchange string, r *router, conn *amqp091.Connection, log zerolog.Logger) *Supervisor {
	return &Supervisor{
		addr:     addr,
		name:     name,
		exchange: exchange,
		router:   r,
		conn:     conn,
		log:      log,
	}
}

// Close releases the connection currently installed. Register it with the
// service's cleanups: after a reconnect the connection holding the live channel
// is not the one the service dialled at startup.
func (s *Supervisor) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn := s.conn
	s.conn = nil

	if conn == nil {
		return nil
	}

	return conn.Close()
}

// Watch runs until ctx is cancelled, re-establishing the connection each time
// it closes. Run it in its own goroutine.
func (s *Supervisor) Watch(ctx context.Context) {
	b := backoff.New(0, 0)

	for {
		conn := s.current()
		if conn == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case err := <-conn.NotifyClose(make(chan *amqp091.Error, 1)):
			s.log.Error().
				Err(err).
				Str("broker", logging.RedactURL(s.addr)).
				Msg("broker connection closed")
		}

		if !s.reconnect(ctx, b) {
			return
		}
	}
}

func (s *Supervisor) current() *amqp091.Connection {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.conn
}

func (s *Supervisor) install(conn *amqp091.Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = conn
}

// reconnect dials until it has installed a usable channel, or ctx is cancelled.
// It reports whether a new connection was installed.
func (s *Supervisor) reconnect(ctx context.Context, b *backoff.Backoff) bool {
	for {
		if err := b.Wait(ctx); err != nil {
			return false
		}

		conn, err := amqp091.Dial(s.addr)
		if err != nil {
			s.log.Warn().
				Err(err).
				Str("broker", logging.RedactURL(s.addr)).
				Msg("broker reconnect failed")

			continue
		}

		if err := s.setup(conn); err != nil {
			s.log.Warn().Err(err).Msg("broker reconnect failed")
			_ = conn.Close()

			continue
		}

		s.install(conn)
		b.Reset()

		s.log.Info().
			Str("broker", logging.RedactURL(s.addr)).
			Msg("broker reconnected")

		return true
	}
}

func (s *Supervisor) setup(conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	qSetup, err := SetupRouter(ch, s.name, s.exchange)
	if err != nil {
		return err
	}

	q, _, _ := qSetup()
	s.router.ReplaceChannel(ch, q)

	return nil
}
