package handlers

import (
	"context"
	"encoding/json"
	"errors"
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
	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}

	userID, err := u.u.AddNewUser(r.Context(), models.User{
		Email:  creds.Email,
		Passwd: creds.Passwd,
	})
	if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("signup handler")

		if errors.Is(err, models.ErrUserExists) {
			http.Error(w, "email is already registered", http.StatusConflict)
			return
		}

		http.Error(w, "unable to signup user", http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"user_id": userID.String(),
	})
}
