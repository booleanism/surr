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
	u   map[tokenType]tokenData
	ttl time.Duration
	mu  sync.RWMutex
}

func NewRefreshTokenStore(ttl time.Duration) *refreshTokenStore {
	return &refreshTokenStore{
		u:   make(map[tokenType]tokenData),
		ttl: ttl,
	}
}

func (r *refreshTokenStore) TTL() time.Duration {
	return r.ttl
}

func (r *refreshTokenStore) SaveRefreshToken(token string, userID uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.u[tokenType(token)] = tokenData{
		userID:    userID,
		expiresAt: time.Now().Add(r.ttl),
	}
	return nil
}

func (r *refreshTokenStore) ValidateRefreshToken(token string) (uuid.UUID, error) {
	r.mu.RLock()
	entry, ok := r.u[tokenType(token)]
	r.mu.RUnlock()

	if !ok {
		return uuid.Nil, errors.New("user session not found")
	}

	if !time.Now().Before(entry.expiresAt) {
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
