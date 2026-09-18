package middlewares

import (
	"net/http"
	"strings"
)

const UserIDHeaderKey = "X-User-ID"

type Auth interface {
	ValidateAccessToken(tokenString string) (userID string, err error)
}

type auth struct {
	a Auth
}

func NewAuth(a Auth) auth {
	return auth{
		a: a,
	}
}

func (a auth) RequireAuth(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "missing authorization header", http.StatusUnauthorized)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "invalid authorization header format", http.StatusUnauthorized)
			return
		}

		userID, err := a.a.ValidateAccessToken(parts[1])
		if err != nil {
			http.Error(w, "unauthorized access token", http.StatusUnauthorized)
			return
		}

		r.Header.Set(UserIDHeaderKey, userID)
		next.ServeHTTP(w, r)
	}
}
