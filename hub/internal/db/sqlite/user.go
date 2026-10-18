package sqlite

import (
	"context"
	"fmt"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
)

type users struct {
	*db.Transaction
}

func NewUsers(txFactory func() *db.Transaction) users {
	return users{
		Transaction: txFactory(),
	}
}

func (ur users) GetUser(ctx context.Context, email string, buf *models.User) error {
	stmt := "SELECT id, email, passwd FROM users WHERE email = ?"
	return ur.DB.QueryRowContext(ctx, stmt, email).Scan(&buf.ID, &buf.Email, &buf.Passwd)
}

func (ur users) InsertUser(ctx context.Context, user models.User) error {
	stmt := "INSERT INTO users (id, email, passwd) VALUES (?, ?, ?)"
	_, err := ur.DB.ExecContext(ctx, stmt, user.ID[:], user.Email, user.Passwd)

	// The email column is UNIQUE, so a second signup for the same address is a
	// constraint failure. Translated here because only this layer sees the
	// driver error, and the handler has to answer 409 rather than 500.
	if db.IsUniqueViolation(err) {
		return fmt.Errorf("%w: %s", models.ErrUserExists, user.Email)
	}

	return err
}

func (ur users) GetUserByID(ctx context.Context, user *models.User) error {
	stmt := "SELECT id, email, passwd FROM users WHERE id = ?"
	return ur.DB.QueryRowContext(ctx, stmt, user.ID[:]).Scan(&user.ID, &user.Email, &user.Passwd)
}
