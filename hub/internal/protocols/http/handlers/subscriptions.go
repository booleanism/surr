package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Subscription interface {
	AddSubscription(ctx context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error)
	RemoveSubscription(ctx context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error)
	GetNodes(ctx context.Context, subs models.Subscription, sinks *[]models.Node) (int, error)
}

type subscriptionHandlers struct {
	m    *http.ServeMux
	s    Subscription
	auth AuthHandlerFunc
}

func NewSubscription(mux *http.ServeMux, s Subscription, auth AuthHandlerFunc) subscriptionHandlers {
	h := subscriptionHandlers{
		m:    mux,
		s:    s,
		auth: auth,
	}

	mux.HandleFunc("POST /add", auth.RequireAuthHandlerFunc(h.add))
	mux.HandleFunc("POST /remove", auth.RequireAuthHandlerFunc(h.remove))
	mux.HandleFunc("GET /sinks", auth.RequireAuthHandlerFunc(h.sinks))
	return h
}

func (s subscriptionHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

func (s subscriptionHandlers) sinks(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	subsID, err := uuid.Parse(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "invalid subscription id", http.StatusForbidden)
		return
	}

	nb := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nb)

	subs := models.Subscription{
		ID: subsID,
	}

	n, err := s.s.GetNodes(r.Context(), subs, nb)
	if err != nil {
		zerolog.Ctx(r.Context()).Error().Str("sub_id", subsID.String()).Err(err).Msg("get nodes")
		http.Error(w, "unable to check subscription id", http.StatusInternalServerError)
		return
	}

	if n == 0 {
		http.Error(w, "no sinks available", http.StatusNoContent)
		return
	}

	_ = json.NewEncoder(w).Encode((*nb)[:n])
}

func (s subscriptionHandlers) add(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	subsNodes := models.SubscribedNodes{}
	err := json.NewDecoder(r.Body).Decode(&subsNodes)
	if err != nil {
		http.Error(w, "unable to decode request", http.StatusBadRequest)
		return
	}

	// userID, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	// if err != nil {
	// 	http.Error(w, "unable to parse userID", http.StatusBadRequest)
	// 	return
	// }

	// subsNodes.User.ID = userID
	subsID, err := s.s.AddSubscription(r.Context(), subsNodes)
	if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("add subscription")
		http.Error(w, "unable to add subscription", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"subscription_id": subsID,
	})
}

func (s subscriptionHandlers) remove(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
