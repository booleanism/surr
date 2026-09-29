// Package logging provides tx's zerolog setup. tx is a standalone module
// and deliberately does not depend on the shared root internal/logging, so
// this is a trimmed copy of it (tx has no broker family). The root logger
// is built in main and injected into the root context; packages pull it
// back out with zerolog.Ctx.
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

// Setup builds the service root logger (service=<name> field, console or
// json output) and returns a context derived from ctx that carries it.
func Setup(ctx context.Context, name, level, format string) (context.Context, zerolog.Logger) {
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

	return out.WithContext(ctx), out
}

// RedactURL reduces a credential-bearing URI (e.g. rtsp://user:pass@host)
// to scheme://host for logging. Unparseable values come back as
// "unparseable-url" so a broken input can never leak secrets.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return "unparseable-url"
	}

	return u.Scheme + "://" + u.Host
}
