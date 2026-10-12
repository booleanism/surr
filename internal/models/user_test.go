package models

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestAPasswordHashIsNotSerializable pins the reason Passwd has no JSON name.
// Once the user is stored the field holds the argon2id hash, and the model is
// marshalled as a broker payload (hub/internal/adapter/rabbitmq sends a User to
// the subscriptions service), so a serializable field would put the hash on the
// wire and into every log that prints a payload.
func TestAPasswordHashIsNotSerializable(t *testing.T) {
	user := User{
		ID:     uuid.New(),
		Email:  "user@example.com",
		Passwd: "$argon2id$v=19$m=65536,t=3,p=2$c2FsdA$aGFzaA",
	}

	encoded, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("marshal user: %v", err)
	}

	if strings.Contains(string(encoded), "argon2id") {
		t.Errorf("the password hash was serialized: %s", encoded)
	}

	if !strings.Contains(string(encoded), "user@example.com") {
		t.Errorf("the email should still travel: %s", encoded)
	}

	// The field must not be settable from a body either: a request that names
	// it would otherwise choose the stored hash.
	var decoded User
	if unmarshalErr := json.Unmarshal([]byte(`{"email":"a@b.c","passwd":"x"}`), &decoded); unmarshalErr != nil {
		t.Fatalf("unmarshal user: %v", unmarshalErr)
	}

	if decoded.Passwd != "" {
		t.Errorf("passwd was decoded from a request body: %q", decoded.Passwd)
	}
}
