package handlers

import (
	"encoding/json"
	"net/http"
)

// credentials is the request body of POST /users/signup and POST /auth/signin.
//
// The handlers used to decode straight into models.User, which had two
// consequences. The password field of the shared model had to stay
// JSON-serializable, so any handler that marshalled a User carried the hash
// with it; and because Go's decoder ignores unknown keys, a client that sent
// "password" -- the name almost every API uses -- got a 201 and an account
// whose password was the empty string, which then authenticated.
type credentials struct {
	Email  string `json:"email"`
	Passwd string `json:"passwd"`
}

// decodeCredentials reads and validates a credentials body.
//
// Unknown fields are rejected so the "password" mistake is a 400 naming the
// problem instead of a silently empty account. It writes the error response
// itself and reports whether the caller may continue.
func decodeCredentials(w http.ResponseWriter, r *http.Request) (credentials, bool) {
	defer r.Body.Close() //nolint:errcheck

	var creds credentials

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&creds); err != nil {
		http.Error(w, "invalid request payload: expected {\"email\": ..., \"passwd\": ...}", http.StatusBadRequest)
		return credentials{}, false
	}

	if creds.Email == "" || creds.Passwd == "" {
		http.Error(w, "email and passwd are required", http.StatusBadRequest)
		return credentials{}, false
	}

	return creds, true
}
