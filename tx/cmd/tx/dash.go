package main

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/booleanism/surr/tx/internal/server/dash"
	"github.com/rs/zerolog"
)

// dashReadTimeout bounds how long a DASH client may take to send its request.
const dashReadTimeout = 15 * time.Second

func httpLogger(log zerolog.Logger, mux *http.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debug().Str("method", r.Method).Str("path", r.RequestURI).Msg("dash request")
		mux.ServeHTTP(w, r)
	}
}

// isLoopbackAddr reports whether a RemoteAddr host is a loopback address.
func isLoopbackAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback()
}

// requireDashToken guards the routes that create or delete a stream.
//
// The DASH server has no user model, and the mutating routes used to be open
// to anyone who could reach the port. A configured token is now always
// required; with no token, only a loopback caller may mutate, so widening the
// bind address cannot silently expose stream creation and deletion.
func requireDashToken(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			if isLoopbackAddr(r.RemoteAddr) {
				next(w, r)
				return
			}

			http.Error(w, "dash token required", http.StatusUnauthorized)

			return
		}

		presented := r.Header.Get("X-Dash-Token")
		if presented == "" {
			presented = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		}

		if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			http.Error(w, "invalid dash token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func setupMuxDashServer(manifestFileName, dashToken string, dash *dash.DashServer) (*http.ServeMux, error) {
	mux := http.NewServeMux()

	p, h, err := dash.Index(mux)
	if err != nil {
		return nil, err
	}

	mux.HandleFunc(p, h)

	p, h, err = dash.PlayStream(manifestFileName)
	if err != nil {
		return nil, err
	}

	mux.HandleFunc(p, h)

	p, h = dash.StreamSegment()
	mux.HandleFunc(p, h)
	p, h = dash.StreamManifest(manifestFileName)
	mux.HandleFunc(p, h)

	// Only these two change the filesystem, so only these two are guarded.
	p, h = dash.AddStream()
	mux.HandleFunc(p, requireDashToken(dashToken, h))
	p, h = dash.DeleteStream()
	mux.HandleFunc(p, requireDashToken(dashToken, h))

	return mux, nil
}

func setupDashServer(addr, manifestFileName, dashToken string, dash *dash.DashServer, log zerolog.Logger) (*http.Server, error) {
	mux, err := setupMuxDashServer(manifestFileName, dashToken, dash)
	if err != nil {
		return nil, err
	}

	s := &http.Server{
		Addr:              addr,
		Handler:           httpLogger(log, mux),
		ReadHeaderTimeout: GeneralTimeout,
		ReadTimeout:       dashReadTimeout,
		// No WriteTimeout: a segment or manifest is streamed to the client and
		// a slow player must not be cut off mid-file.
		IdleTimeout: GeneralTimeout * 12,
	}

	return s, nil
}
