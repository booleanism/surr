package models

import "github.com/google/uuid"

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
