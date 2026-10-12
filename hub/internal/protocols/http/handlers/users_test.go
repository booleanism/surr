package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// The field order is not free: govet's fieldalignment wants the pointer-bearing
// field (the error interface) packed first.
type fakeUsers struct {
	err   error
	added []models.User
	id    uuid.UUID
}

func (f *fakeUsers) AddNewUser(_ context.Context, u models.User) (uuid.UUID, error) {
	f.added = append(f.added, u)

	if f.err != nil {
		return uuid.Nil, f.err
	}

	return f.id, nil
}

func postSignup(t *testing.T, fake *fakeUsers, body string) *httptest.ResponseRecorder {
	t.Helper()

	h := NewUser(http.NewServeMux(), fake)

	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.signUpHandler(rec, req)

	return rec
}

// TestSignupRejectsAPasswordUnderTheWrongName is the reason unknown fields are
// refused: the wire name is `passwd`, and a client that sends `password` -- the
// name almost every API uses -- used to have the key dropped, which created an
// account with an empty password that then authenticated.
func TestSignupRejectsAPasswordUnderTheWrongName(t *testing.T) {
	fake := &fakeUsers{id: uuid.New()}

	rec := postSignup(t, fake, `{"email":"a@b.c","password":"secret"}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a user was created from an unrecognised body: %+v", fake.added)
	}
}

// The body here carries a valid passwd as well, so the 400 can only come from
// the unrecognised key. Without it the empty-password check below would reject
// the request anyway and this test would pass for the wrong reason.
func TestSignupRejectsAnUnknownFieldBesideAValidPassword(t *testing.T) {
	fake := &fakeUsers{id: uuid.New()}

	rec := postSignup(t, fake, `{"email":"a@b.c","passwd":"secret","password":"secret"}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a user was created from a body with an unknown field: %+v", fake.added)
	}
}

func TestSignupRejectsAnEmptyPassword(t *testing.T) {
	fake := &fakeUsers{id: uuid.New()}

	rec := postSignup(t, fake, `{"email":"a@b.c","passwd":""}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a user was created with an empty password: %+v", fake.added)
	}
}

func TestSignupRejectsAnEmptyEmail(t *testing.T) {
	fake := &fakeUsers{id: uuid.New()}

	rec := postSignup(t, fake, `{"passwd":"secret"}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a user was created without an email: %+v", fake.added)
	}
}

func TestSignupCreatesTheUser(t *testing.T) {
	id := uuid.New()
	fake := &fakeUsers{id: id}

	rec := postSignup(t, fake, `{"email":"a@b.c","passwd":"secret"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status is %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if len(fake.added) != 1 {
		t.Fatalf("AddNewUser was called %d times, want 1", len(fake.added))
	}

	if fake.added[0].Email != "a@b.c" || fake.added[0].Passwd != "secret" {
		t.Errorf("AddNewUser received %+v", fake.added[0])
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body["user_id"] != id.String() {
		t.Errorf("user_id is %q, want %q", body["user_id"], id.String())
	}
}

// A duplicate email is a conflict between the request and the stored state, not
// a server failure, and the storage layer is what can tell the difference.
func TestSignupReportsADuplicateEmailAsConflict(t *testing.T) {
	fake := &fakeUsers{err: fmt.Errorf("%w: a@b.c", models.ErrUserExists)}

	rec := postSignup(t, fake, `{"email":"a@b.c","passwd":"secret"}`)

	if rec.Code != http.StatusConflict {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusConflict)
	}
}
