package models

import (
	"errors"

	"github.com/google/uuid"
)

// ErrSubscriptionNotFound reports that no subscription matched the requested id
// for the requesting user.
//
// It lives here rather than in either service because it crosses the broker:
// the subscriptions service enforces ownership in SQL and replies with
// protocols.CodeNotFound, and the hub turns that back into this error so its
// HTTP layer can answer 403 instead of 500.
var ErrSubscriptionNotFound = errors.New("subscription not found")

type Node struct {
	ID      uuid.UUID `json:"id,omitzero"`
	Bitrate int64     `json:"bitrate,omitempty"`
	Width   int64     `json:"width,omitempty"`
	Height  int64     `json:"height,omitempty"`
}

type Subscription struct {
	User `json:"user"`
	ID   uuid.UUID `json:"id,omitzero"`
}

type SubscribedNodes struct {
	Nodes        []Node `json:"nodes"`
	Subscription `json:"subscription"`
	ID           uuid.UUID `json:"id,omitzero"`
}
