package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

type ArgonParams struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

type argonHashing struct {
	p ArgonParams
}

func NewArgonHashing(p ArgonParams) *argonHashing {
	return &argonHashing{
		p: p,
	}
}

func (a *argonHashing) HashPassword(password string) (string, error) {
	salt := make([]byte, a.p.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	hash := argon2.IDKey([]byte(password), salt, a.p.Iterations, a.p.Memory, a.p.Parallelism, a.p.KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encoded := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, a.p.Memory, a.p.Iterations, a.p.Parallelism, b64Salt, b64Hash)

	return encoded, nil
}

func (a *argonHashing) VerifyPassword(password, encodedHash string) error {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 {
		return fmt.Errorf("invalid hash format")
	}

	if parts[1] != "argon2id" {
		return fmt.Errorf("unsupported algorithm variant")
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return err
	}
	if version != argon2.Version {
		return fmt.Errorf("incompatible version")
	}

	p := &ArgonParams{}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.Memory, &p.Iterations, &p.Parallelism); err != nil {
		return err
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return err
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return err
	}

	// p.SaltLength = uint32(len(salt))
	p.KeyLength = uint32(len(expectedHash)) //nolint:gosec

	comparisonHash := argon2.IDKey([]byte(password), salt, p.Iterations, p.Memory, p.Parallelism, p.KeyLength)

	if subtle.ConstantTimeCompare(expectedHash, comparisonHash) == 1 {
		return nil
	}

	return fmt.Errorf("not match")
}
