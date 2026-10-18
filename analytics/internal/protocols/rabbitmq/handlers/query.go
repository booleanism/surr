package handlers

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/booleanism/surr/analytics/internal/usecases"
	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Queries is the query usecase surface these handlers need. Declared here, in
// the consumer, for the same reason as Declarations.
type Queries interface {
	Execute(ctx context.Context, req models.AnalyticsQueryRequest, out *json.RawMessage) error
}

type query struct {
	h SetHandler
	q Queries
}

// SetupQuery registers the command that runs a declared query.
func SetupQuery(queries Queries, h SetHandler) error {
	q := query{
		h: h,
		q: queries,
	}

	return q.h.Set("analytics.query", q.queryHandler)
}

// queryHandler answers one named query for one node.
//
// The caller names a query the node declared; it never supplies a pipeline. That
// is what keeps the aggregation surface the node's to define and this service's
// to enforce: the worst a caller can ask for is a query that was already
// declared, already validated and already capped.
func (q query) queryHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "analytics.query"

	log := zerolog.Ctx(ctx)
	log.Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	request := models.AnalyticsQueryRequest{}

	err := json.Unmarshal(req.Payload, &request)
	if err != nil {
		q.fail(ctx, command, protocols.CodeDecodePayload, err, rep)

		return
	}

	log.Debug().
		Str("node_id", request.NodeID.String()).
		Str("query_name", request.QueryName).
		Int("params", len(request.Params)).
		Msg("running declared query")

	var out json.RawMessage

	err = q.q.Execute(ctx, request, &out)
	if err != nil {
		q.fail(ctx, command, queryCode(err), err, rep)

		return
	}

	_ = rep(ctx, protocols.ResultResponse(out))
}

// fail logs and answers with one reply code, so every failure path reports the
// same way and none of them can forget the log line.
func (q query) fail(ctx context.Context, command string, code int, err error, rep protocols.ReplyFunc) {
	zerolog.Ctx(ctx).Error().Str("command", command).Int("code", code).Err(err).Msg("handler failed")
	_ = rep(ctx, protocols.ErrorResponse(code, err))
}

// queryCode maps a query failure onto the wire.
//
// The split is by whose mistake it is. A missing declaration is not found. A
// pipeline or parameter the manifest and the call disagree about is a payload
// problem, because the request that produced it did not match the contract the
// node published. Anything else -- a database failure, a result over the cap --
// is this service's own domain failure, and the message says which.
func queryCode(err error) int {
	switch {
	case errors.Is(err, models.ErrDeclarationNotFound):
		return protocols.CodeNotFound
	case errors.Is(err, usecases.ErrInvalidPipeline),
		errors.Is(err, usecases.ErrUnknownParam),
		errors.Is(err, usecases.ErrMissingParam),
		errors.Is(err, usecases.ErrInvalidDeclaration):
		return protocols.CodeDecodePayload
	default:
		return protocols.CodeDomain
	}
}
