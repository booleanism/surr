package middlewares

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

// statusWriter captures the response status code so the request logger can
// report it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	return w.ResponseWriter.Write(b)
}

// NewRequestLogger wraps next with a status-capturing middleware that logs
// every request (method, path, status, duration_ms, remote) at a level
// derived from the status: 2xx debug, 4xx warn, 5xx error. It also injects
// the root logger into the request context so downstream handlers can pull
// it with zerolog.Ctx(r.Context()).
func NewRequestLogger(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCtx := log.WithContext(r.Context())
			r = r.WithContext(reqCtx)

			start := time.Now()
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r)
			if sw.status == 0 {
				sw.status = http.StatusOK
			}

			evt := log.With().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Int("status", sw.status).
				Int64("duration_ms", time.Since(start).Milliseconds()).
				Str("remote", r.RemoteAddr).
				Logger()

			switch {
			case sw.status >= http.StatusInternalServerError:
				evt.Error().Msg("http request")
			case sw.status >= http.StatusBadRequest:
				evt.Warn().Msg("http request")
			default:
				evt.Debug().Msg("http request")
			}
		})
	}
}
