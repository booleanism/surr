package usecases

import (
	"context"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type UserHash interface {
	HashPassword(password string) (string, error)
}

type User interface {
	InsertUser(ctx context.Context, user models.User) error
}

type user struct {
	u User
	h UserHash
}

func NewUser(u User, hash UserHash) user {
	return user{
		u: u,
		h: hash,
	}
}

func (u user) AddNewUser(ctx context.Context, user models.User) (uuid.UUID, error) {
	hashed, err := u.h.HashPassword(user.Passwd)
	if err != nil {
		return uuid.Nil, err
	}

	userID, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, err
	}

	um := models.User{
		ID:     userID,
		Email:  user.Email,
		Passwd: hashed,
	}

	return userID, u.u.InsertUser(ctx, um)
}
