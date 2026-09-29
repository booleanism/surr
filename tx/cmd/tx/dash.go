package main

import (
	"net/http"

	"github.com/booleanism/surr/tx/internal/server/dash"
	"github.com/rs/zerolog"
)

func httpLogger(log zerolog.Logger, mux *http.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debug().Str("method", r.Method).Str("path", r.RequestURI).Msg("dash request")
		mux.ServeHTTP(w, r)
	}
}

func setupMuxDashServer(manifestFileName string, dash *dash.DashServer) (*http.ServeMux, error) {
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
	p, h = dash.AddStream()
	mux.HandleFunc(p, h)
	p, h = dash.DeleteStream()
	mux.HandleFunc(p, h)

	return mux, nil
}

func setupDashServer(addr, manifestFileName string, dash *dash.DashServer, log zerolog.Logger) (*http.Server, error) {
	mux, err := setupMuxDashServer(manifestFileName, dash)
	if err != nil {
		return nil, err
	}

	s := &http.Server{
		Addr:              addr,
		Handler:           httpLogger(log, mux),
		ReadHeaderTimeout: GeneralTimeout,
	}

	return s, nil
}
