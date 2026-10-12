package handlers

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	"github.com/booleanism/surr/hub/internal/usecases/auth"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
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
	m       *http.ServeMux
	s       RefreshTokenStore
	a       Authenticator
	limiter *middlewares.RateLimiter

	jwtTTL       time.Duration
	secureCookie bool
}

// Signin rate limiting: a caller that fails or repeats signin is throttled per
// client address. Argon2 verification is deliberately expensive, so an
// unthrottled endpoint is both a credential-stuffing target and a CPU
// amplifier.
const (
	SigninAttemptsPerWindow = 10
	SigninWindow            = time.Minute
)

// NewAuthHandlers registers the auth routes.
//
// secureCookie controls the Secure attribute on the refresh cookie: leave it
// true for anything reachable over TLS, and set it false only for a plain-HTTP
// local deployment, where a browser would otherwise refuse to store the cookie.
func NewAuthHandlers(mux *http.ServeMux, s RefreshTokenStore, a Authenticator, jwtTTL time.Duration, secureCookie bool) authHandlers {
	ah := authHandlers{
		m:            mux,
		s:            s,
		a:            a,
		limiter:      middlewares.NewRateLimiter(SigninAttemptsPerWindow, SigninWindow),
		jwtTTL:       jwtTTL,
		secureCookie: secureCookie,
	}

	mux.HandleFunc("POST /signin", ah.signinHandler)
	mux.HandleFunc("POST /refresh", ah.refreshAccessTokenHandler)
	mux.HandleFunc("POST /logout", ah.logoutHandler)

	return ah
}

func (h authHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.m.ServeHTTP(w, r)
}

// refreshCookie builds the refresh cookie so signin, refresh and logout cannot
// drift apart in their attributes.
func (h authHandlers) refreshCookie(value string, expires time.Time) *http.Cookie {
	return &http.Cookie{ //nolint:gosec // HttpOnly and SameSite are always set; Secure follows --cookie-secure.
		Name:     "refresh_token",
		Value:    value,
		Path:     "/auth",
		HttpOnly: true,
		Secure:   h.secureCookie,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	}
}

func (h authHandlers) signinHandler(w http.ResponseWriter, r *http.Request) {
	log := zerolog.Ctx(r.Context())

	// Key on the client address, not on the submitted email: keying on the
	// email would let an attacker lock a known account out.
	if allowed, retryAfter := h.limiter.Allow(clientAddr(r)); !allowed {
		log.Warn().Dur("retry_after", retryAfter).Msg("signin rate limited")
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
		http.Error(w, "too many signin attempts", http.StatusTooManyRequests)
		return
	}

	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}

	userID, err := h.a.VerifyCredentials(r.Context(), auth.UserAuth{
		Email:  creds.Email,
		Passwd: creds.Passwd,
	})
	if err != nil {
		// The submitted address is not logged: a failed signin would otherwise
		// write user identifiers to disk in cleartext.
		log.Warn().Err(err).Msg("signin failed")
		http.Error(w, "invalid email or password", http.StatusUnauthorized)
		return
	}

	log.Info().Str("user_id", userID.String()).Msg("signin success")

	tokens, err := h.a.GenerateTokenPair(userID)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err = h.s.SaveRefreshToken(tokens.RefreshToken, userID); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, h.refreshCookie(tokens.RefreshToken, time.Now().Add(h.s.TTL())))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": tokens.AccessToken,
		"token_type":   "Bearer",
		"expires_in":   h.jwtTTL.Seconds(),
	})
}

// clientAddr returns the host part of the request's remote address, so the
// limiter is not defeated by a client that cycles source ports.
func clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func (h authHandlers) refreshAccessTokenHandler(w http.ResponseWriter, r *http.Request) {
	log := zerolog.Ctx(r.Context())

	cookie, err := r.Cookie("refresh_token")
	if err != nil {
		http.Error(w, "missing refresh token", http.StatusUnauthorized)
		return
	}

	userID, err := h.s.ValidateRefreshToken(cookie.Value)
	if err != nil {
		log.Warn().Msg("invalid or revoked refresh token")
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

	log.Info().Str("user_id", userID.String()).Msg("refresh token rotated")

	http.SetCookie(w, h.refreshCookie(newTokens.RefreshToken, time.Now().Add(h.s.TTL())))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": newTokens.AccessToken,
		"token_type":   "Bearer",
		"expires_in":   h.jwtTTL.Seconds(),
	})
}

func (h authHandlers) logoutHandler(w http.ResponseWriter, r *http.Request) {
	log := zerolog.Ctx(r.Context())

	cookie, err := r.Cookie("refresh_token")
	if err == nil {
		if userID, valErr := h.s.ValidateRefreshToken(cookie.Value); valErr == nil {
			_ = h.s.DeleteRefreshToken(cookie.Value, userID)
			log.Info().Str("user_id", userID.String()).Msg("logout")
		}
	}

	http.SetCookie(w, &http.Cookie{ //nolint:gosec // HttpOnly and SameSite are always set; Secure follows --cookie-secure.
		Name:     "refresh_token",
		Value:    "",
		Path:     "/auth",
		HttpOnly: true,
		Secure:   h.secureCookie,
		// SameSite matches the cookie set at signin: a Strict cookie is not
		// sent on a top-level navigation from another site, so the value being
		// cleared would be dropped by the browser in exactly the case where the
		// server already deleted it from its store.
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})

	w.WriteHeader(http.StatusNoContent)
}
