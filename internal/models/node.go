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

// ErrNodeAlreadySubscribed reports that a node is already attached to another
// subscription.
//
// A node belongs to exactly one subscription -- subscribed_nodes.node_id is
// UNIQUE -- and no endpoint moves one, so the second add is a conflict rather
// than a repeat of the first. It lives here for the same reason as
// ErrSubscriptionNotFound: the subscriptions service sees only a SQLite
// constraint failure and the hub has to turn that into 409.
var ErrNodeAlreadySubscribed = errors.New("node is already subscribed")

// ErrNodeUnavailable reports that a node is not advertising itself on the
// broker, so a request that would be answered from its stored data would be
// answered from a node that will never write again.
//
// The hub knows this from the sibling registry, not from its `nodes` table: a
// row there cannot be dated (`nodes.add` is sent once per run and carries no
// timestamp), so a node killed without a graceful `nodes.remove` leaves a row
// that is indistinguishable from a live one. Answering an analytics report for
// such a row is the failure mode this guards: every query comes back with its
// empty shape, which reads as "this node saw nothing" rather than "this node is
// gone" (TODO.md F6).
var ErrNodeUnavailable = errors.New("node is not running")

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
