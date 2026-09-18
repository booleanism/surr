package models

import "github.com/google/uuid"

type WorkerSources struct {
	Remotes        []Node    `json:"remotes"`
	SubscriptionID uuid.UUID `json:"subscription_id"`
}

type WorkerSink struct {
	SinkAddress string    `json:"sink_address"`
	WorkerID    uuid.UUID `json:"worker_id"`
}
