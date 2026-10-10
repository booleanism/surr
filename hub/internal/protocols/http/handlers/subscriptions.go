package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Subscription interface {
	AddSubscription(ctx context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error)
	RemoveSubscription(ctx context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error)
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
	return h
}

func (s subscriptionHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

func (s subscriptionHandlers) add(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	log := zerolog.Ctx(r.Context())
	subsNodes := models.SubscribedNodes{}

	err := json.NewDecoder(r.Body).Decode(&subsNodes)
	if err != nil {
		log.Warn().Err(err).Msg("decoding request")
		http.Error(w, "decoding request", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	if err != nil {
		log.Warn().Err(err).Msg("parsing userID")
		http.Error(w, "parsing userID", http.StatusBadRequest)
		return
	}

	subsNodes.User.ID = uid
	subsID, err := s.s.AddSubscription(r.Context(), subsNodes)
	if err != nil {
		log.Error().Err(err).Msg("add subscription")
		http.Error(w, "unable to add subscription", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"subscription_id": subsID,
	})
}

func (s subscriptionHandlers) remove(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	log := zerolog.Ctx(r.Context())
	subsNodes := models.SubscribedNodes{}

	err := json.NewDecoder(r.Body).Decode(&subsNodes)
	if err != nil {
		log.Warn().Err(err).Msg("decoding request")
		http.Error(w, "decoding request", http.StatusBadRequest)
		return
	}

	if subsNodes.Subscription.ID == uuid.Nil {
		log.Warn().Msg("unexpected subscription_id")
		http.Error(w, "unexpected subscription_id", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	if err != nil {
		log.Warn().Err(err).Msg("parsing userID")
		http.Error(w, "parsing userID", http.StatusBadRequest)
		return
	}

	subsNodes.User.ID = uid
	subsID, err := s.s.RemoveSubscription(r.Context(), subsNodes)
	if err != nil {
		// The subscription service refuses a delete it cannot match against
		// the caller's user id; report that as forbidden rather than as an
		// internal failure.
		if errors.Is(err, models.ErrSubscriptionNotFound) {
			log.Warn().Err(err).Msg("remove subscription")
			http.Error(w, "subscription not found", http.StatusForbidden)
			return
		}

		log.Error().Err(err).Msg("remove subscription")
		http.Error(w, "unable to remove subscription", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"subscription_id": subsID,
	})
}
