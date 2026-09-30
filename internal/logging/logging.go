// Package logging provides the shared zerolog setup for the broker-connected
// Go services (hub, rx, subscriptions). Each main builds a root logger via
// Setup, injects it into the root context, and shared packages pull it back
// out with zerolog.Ctx — which degrades to a disabled logger when absent
// (e.g. in tests), so packages stay usable without one.
package logging

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Flag values accepted by the --log-level / --log-format flags.
const (
	DefaultLevel  = "info"
	DefaultFormat = "console"

	FormatConsole = "console"
	FormatJSON    = "json"
)

// ParseLevel maps a --log-level flag value onto a zerolog level. Unknown
// values fall back to the default info level.
func ParseLevel(level string) zerolog.Level {
	l, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil {
		return zerolog.InfoLevel
	}

	return l
}

// Setup builds the service root logger (service=<name> family=<family>
// fields, console or json output) and returns a context derived from ctx
// that carries it. It is called once from each service's main; everything
// downstream reads the logger via zerolog.Ctx.
func Setup(ctx context.Context, name, family, level, format string) (context.Context, zerolog.Logger) {
	var out zerolog.Logger
	if format == FormatJSON {
		out = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		out = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}).With().Timestamp().Logger()
	}

	out = out.With().
		Str("service", name).
		Logger().
		Level(ParseLevel(level))

	if family != "" {
		out = out.With().Str("family", family).Logger()
	}

	return out.WithContext(ctx), out
}

// RedactURL reduces a credential-bearing URI (amqp://user:pass@host,
// rtsp://user:pass@host) to scheme://host for logging. Unparseable values
// come back as "unparseable-url" so a broken input can never leak secrets.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return "unparseable-url"
	}

	return u.Scheme + "://" + u.Host
}
