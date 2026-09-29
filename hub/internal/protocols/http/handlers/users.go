package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type User interface {
	AddNewUser(ctx context.Context, u models.User) (uuid.UUID, error)
}

type userHandlers struct {
	m *http.ServeMux
	u User
}

func NewUser(mux *http.ServeMux, user User) userHandlers {
	h := userHandlers{
		m: mux,
		u: user,
	}

	mux.HandleFunc("POST /signup", h.signUpHandler)
	return h
}

func (s userHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

func (u userHandlers) signUpHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	var req models.User
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	userID, err := u.u.AddNewUser(r.Context(), req)
	if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("signup handler")
		http.Error(w, "unable to signup user", http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"user_id": userID.String(),
	})
}
