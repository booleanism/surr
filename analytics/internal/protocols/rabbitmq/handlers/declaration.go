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

// SetHandler is the router's handler registry. Declaring it here rather than
// taking *protocols.Handlers keeps this package testable with a fake, and keeps
// the dependency pointing in one direction.
type SetHandler interface {
	Set(c protocols.Command, h protocols.HandlerFunc) error
}

// Declarations is the usecase surface these handlers need. It is declared here,
// in the consumer, so the usecase package does not have to name its own callers
// -- the same split as subscriptions' handlers.Subscriptions.
type Declarations interface {
	Declare(ctx context.Context, decl models.Declaration) error
	ListQueries(ctx context.Context, nodeID uuid.UUID, names *[]string) (int, error)
}

type declaration struct {
	h SetHandler
	d Declarations
}

// SetupDeclaration registers the commands this service serves.
//
// Every command registered here is advertised in introduce, which is how a peer
// resolves it to this service -- so a command with no registration is a command
// no node can call, and one registered by mistake is a command that resolves to
// a handler that does not exist.
func SetupDeclaration(decl Declarations, h SetHandler) error {
	d := declaration{
		h: h,
		d: decl,
	}

	err := d.h.Set("analytics.declare", d.declareHandler)
	if err != nil {
		return err
	}

	return d.h.Set("analytics.queries.list", d.listQueriesHandler)
}

func (d declaration) logFail(ctx context.Context, command string, code int, err error) {
	zerolog.Ctx(ctx).Error().Str("command", command).Int("code", code).Err(err).Msg("handler failed")
}

// declareHandler stores a node's analytics manifest.
//
// The node id travels inside the payload rather than in the command name, which
// is the opposite of the sink commands: sink.start is addressed to one node, so
// the id belongs in the routing key, while a declaration is a fact a node
// publishes about itself and the manifest already names it.
func (d declaration) declareHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "analytics.declare"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	decl := models.Declaration{}

	err := json.Unmarshal(req.Payload, &decl)
	if err != nil {
		d.logFail(ctx, command, protocols.CodeDecodePayload, err)
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeDecodePayload, err))

		return
	}

	err = d.d.Declare(ctx, decl)
	if err != nil {
		// A rejected manifest is the node's mistake, not this service's, so it is
		// reported as CodeDecodePayload: the payload arrived intact but this
		// service will not use it. No new code is introduced for it, because the
		// codes are wire protocol and the detail a node author needs is in the
		// message, which ErrorResponse carries verbatim.
		code := protocols.CodeDomain
		if errors.Is(err, usecases.ErrInvalidDeclaration) {
			code = protocols.CodeDecodePayload
		}

		d.logFail(ctx, command, code, err)
		_ = rep(ctx, protocols.ErrorResponse(code, err))

		return
	}

	res, err := json.Marshal(&decl.Queries)
	if err != nil {
		d.logFail(ctx, command, protocols.CodeMarshalReply, err)
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeMarshalReply, err))

		return
	}

	_ = rep(ctx, protocols.ResultResponse(res))
}

// listQueriesHandler answers which query names a node has declared.
func (d declaration) listQueriesHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "analytics.queries.list"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	ref := models.NodeRef{}

	err := json.Unmarshal(req.Payload, &ref)
	if err != nil {
		d.logFail(ctx, command, protocols.CodeDecodePayload, err)
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeDecodePayload, err))

		return
	}

	names := make([]string, 0, 8)

	n, err := d.d.ListQueries(ctx, ref.NodeID, &names)
	if err != nil {
		d.logFail(ctx, command, protocols.CodeDomain, err)
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeDomain, err))

		return
	}

	res, err := json.Marshal(names[:n])
	if err != nil {
		d.logFail(ctx, command, protocols.CodeMarshalReply, err)
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeMarshalReply, err))

		return
	}

	_ = rep(ctx, protocols.ResultResponse(res))
}
