package models

import (
	"errors"

	"github.com/google/uuid"
)

// ErrUserExists reports that the email is already registered.
//
// It lives here because it crosses the storage/HTTP boundary: SQLite reports
// only a UNIQUE constraint failure, and the signup handler has to answer 409
// rather than 500. The translation happens in hub/internal/db/sqlite, the
// interpretation in hub/internal/protocols/http/handlers.
var ErrUserExists = errors.New("user already exists")

type User struct {
	Email string `json:"email,omitempty"`
	// Passwd holds the argon2id hash once the user is stored, and is
	// deliberately not serializable: it used to be tagged `json:"passwd"`, so
	// every handler that marshalled a User -- including the broker payload
	// hub/internal/adapter/rabbitmq sends to the subscriptions service --
	// carried the hash with it. Request bodies are decoded into the HTTP
	// layer's own credentials type instead.
	Passwd string    `json:"-"`
	ID     uuid.UUID `json:"id,omitzero"`
}
