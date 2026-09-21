package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type JwtClaims struct {
	UserID uuid.UUID `json:"user_id"`
	jwt.RegisteredClaims
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type UserAuth struct {
	Email  string
	Passwd string
}

type AuthenticatorUser interface {
	GetUser(ctx context.Context, emailOrUname string, buf *models.User) error
}

type authenticator struct {
	ur  AuthenticatorUser
	ar  *argonHashing
	s   []byte
	exp time.Duration
}

func NewAuthenticator(ur AuthenticatorUser, ar *argonHashing, s []byte, exp time.Duration) authenticator {
	return authenticator{
		ur:  ur,
		ar:  ar,
		s:   s,
		exp: exp,
	}
}

func (a authenticator) VerifyCredentials(ctx context.Context, u UserAuth) (uuid.UUID, error) {
	user := models.User{}
	err := a.ur.GetUser(ctx, u.Email, &user)
	if err != nil {
		return uuid.Nil, err
	}

	err = a.ar.VerifyPassword(u.Passwd, user.Passwd)
	if err != nil {
		return uuid.Nil, err
	}

	return user.ID, nil
}

func (a authenticator) GenerateTokenPair(userID uuid.UUID) (TokenPair, error) {
	claims := JwtClaims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(a.exp)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "surr",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	accessToken, err := token.SignedString(a.s)
	if err != nil {
		return TokenPair{}, fmt.Errorf("signing access token: %w", err)
	}

	randomBytes := make([]byte, 32)
	if _, err = rand.Read(randomBytes); err != nil {
		return TokenPair{}, fmt.Errorf("generating refresh token: %w", err)
	}

	refreshToken := hex.EncodeToString(randomBytes)
	return TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

func (a authenticator) ValidateAccessToken(tokenString string) (uuid.UUID, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JwtClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing algorithm: %v", token.Header["alg"])
		}

		return a.s, nil
	})
	if err != nil {
		return uuid.Nil, err
	}

	claims, ok := token.Claims.(*JwtClaims)
	if !ok || !token.Valid {
		return uuid.Nil, fmt.Errorf("invalid or expired token")
	}

	return claims.UserID, nil
}
