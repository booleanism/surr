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
	jwt.RegisteredClaims
	UserID uuid.UUID `json:"user_id"`
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

// TokenIssuer is the value written to the JWT `iss` claim and required on
// validation.
const TokenIssuer = "surr"

// signingMethod is the only algorithm tokens are signed with. Validation pins
// it, because accepting any HMAC method lets an attacker pick the weakest one
// the shared secret supports.
var signingMethod = jwt.SigningMethodHS256

func (a authenticator) GenerateTokenPair(userID uuid.UUID) (TokenPair, error) {
	claims := JwtClaims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(a.exp)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    TokenIssuer,
		},
	}

	token := jwt.NewWithClaims(signingMethod, claims)
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
		// Exact match, not a type assertion: HS384/HS512 are also HMAC and
		// would otherwise be accepted against the same secret.
		if token.Method != signingMethod {
			return nil, fmt.Errorf("unexpected signing algorithm: %v", token.Header["alg"])
		}

		return a.s, nil
	},
		jwt.WithValidMethods([]string{signingMethod.Alg()}),
		jwt.WithIssuer(TokenIssuer),
	)
	if err != nil {
		return uuid.Nil, err
	}

	claims, ok := token.Claims.(*JwtClaims)
	if !ok || !token.Valid {
		return uuid.Nil, fmt.Errorf("invalid or expired token")
	}

	return claims.UserID, nil
}
