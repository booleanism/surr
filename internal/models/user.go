package models

import "github.com/google/uuid"

type User struct {
	Email  string    `json:"email,omitempty"`
	Passwd string    `json:"passwd,omitempty"`
	ID     uuid.UUID `json:"id,omitzero"`
}
