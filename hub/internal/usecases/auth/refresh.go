package auth

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type tokenType string

type tokenData struct {
	expiresAt time.Time
	userID    uuid.UUID
}

type refreshTokenStore struct {
	u         map[tokenType]tokenData
	ttl       time.Duration
	lastSweep time.Time
	mu        sync.RWMutex
}

// sweepInterval amortises the expiry sweep: every SaveRefreshToken would
// otherwise walk the whole map.
const sweepInterval = time.Minute

func NewRefreshTokenStore(ttl time.Duration) *refreshTokenStore {
	return &refreshTokenStore{
		u:   make(map[tokenType]tokenData),
		ttl: ttl,
	}
}

func (r *refreshTokenStore) TTL() time.Duration {
	return r.ttl
}

// sweepLocked drops every expired entry. Callers must hold r.mu for writing.
// Without it the in-memory store grows unbounded: expired tokens are rejected
// on validation but were never removed.
func (r *refreshTokenStore) sweepLocked(now time.Time) {
	for t, d := range r.u {
		if !now.Before(d.expiresAt) {
			delete(r.u, t)
		}
	}
}

func (r *refreshTokenStore) SaveRefreshToken(token string, userID uuid.UUID) error {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	if now.Sub(r.lastSweep) >= sweepInterval {
		r.sweepLocked(now)
		r.lastSweep = now
	}

	r.u[tokenType(token)] = tokenData{
		userID:    userID,
		expiresAt: now.Add(r.ttl),
	}

	return nil
}

func (r *refreshTokenStore) ValidateRefreshToken(token string) (uuid.UUID, error) {
	now := time.Now()

	r.mu.RLock()
	entry, ok := r.u[tokenType(token)]
	r.mu.RUnlock()

	if !ok {
		return uuid.Nil, errors.New("user session not found")
	}

	if !now.Before(entry.expiresAt) {
		r.mu.Lock()
		delete(r.u, tokenType(token))
		r.mu.Unlock()

		return uuid.Nil, fmt.Errorf("refresh token expired")
	}

	return entry.userID, nil
}

func (r *refreshTokenStore) DeleteRefreshToken(token string, userID uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.u[tokenType(token)]; ok && bytes.Equal(entry.userID[:], userID[:]) {
		delete(r.u, tokenType(token))
	}

	return nil
}
