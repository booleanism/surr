package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/booleanism/surr/hub/internal/usecases/auth"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type Authenticator interface {
	VerifyCredentials(ctx context.Context, u auth.UserAuth) (uuid.UUID, error)
	GenerateTokenPair(userID uuid.UUID) (auth.TokenPair, error)
}

type RefreshTokenStore interface {
	SaveRefreshToken(token string, userID uuid.UUID) error
	ValidateRefreshToken(token string) (userID uuid.UUID, err error)
	DeleteRefreshToken(token string, userID uuid.UUID) error
	TTL() time.Duration
}

type authHandlers struct {
	m *http.ServeMux
	s RefreshTokenStore
	a Authenticator

	jwtTTL time.Duration
}

func NewAuthHandlers(mux *http.ServeMux, s RefreshTokenStore, a Authenticator, jwtTTL time.Duration) authHandlers {
	ah := authHandlers{
		m:      mux,
		s:      s,
		a:      a,
		jwtTTL: jwtTTL,
	}

	mux.HandleFunc("POST /signin", ah.signinHandler)
	mux.HandleFunc("POST /refresh", ah.refreshAccessTokenHandler)
	mux.HandleFunc("POST /logout", ah.logoutHandler)

	return ah
}

func (h authHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.m.ServeHTTP(w, r)
}

func (h authHandlers) signinHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() //nolint:errcheck

	var req models.User
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	userID, err := h.a.VerifyCredentials(r.Context(), auth.UserAuth{
		Email:  req.Email,
		Passwd: req.Passwd,
	})
	if err != nil {
		http.Error(w, "invalid email or password", http.StatusUnauthorized)
		return
	}

	tokens, err := h.a.GenerateTokenPair(userID)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err = h.s.SaveRefreshToken(tokens.RefreshToken, userID); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "refresh_token",
		Value:    tokens.RefreshToken,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(h.s.TTL()),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": tokens.AccessToken,
		"token_type":   "Bearer",
		"expires_in":   h.jwtTTL.Seconds(),
	})
}

func (h authHandlers) refreshAccessTokenHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refresh_token")
	if err != nil {
		http.Error(w, "missing refresh token", http.StatusUnauthorized)
		return
	}

	userID, err := h.s.ValidateRefreshToken(cookie.Value)
	if err != nil {
		http.Error(w, "invalid or revoked refresh token", http.StatusUnauthorized)
		return
	}

	newTokens, err := h.a.GenerateTokenPair(userID)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	_ = h.s.DeleteRefreshToken(cookie.Value, userID)
	_ = h.s.SaveRefreshToken(newTokens.RefreshToken, userID)

	http.SetCookie(w, &http.Cookie{
		Name:     "refresh_token",
		Value:    newTokens.RefreshToken,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(h.s.TTL()),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": newTokens.AccessToken,
		"token_type":   "Bearer",
		"expires_in":   h.jwtTTL.Seconds(),
	})
}

func (h authHandlers) logoutHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refresh_token")
	if err == nil {
		if userID, valErr := h.s.ValidateRefreshToken(cookie.Value); valErr == nil {
			_ = h.s.DeleteRefreshToken(cookie.Value, userID)
		}
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "refresh_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSite(http.SameSiteStrictMode),
		MaxAge:   -1,
	})

	w.WriteHeader(http.StatusNoContent)
}
