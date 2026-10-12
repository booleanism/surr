package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// introRouter builds a router whose only usable parts are the sibling registry
// and the logger, which is all handlerIntroCommand touches.
func introRouter(t *testing.T) (*router, *bytes.Buffer, Siblings) {
	t.Helper()

	sibs := protocols.NewSiblings()
	q := func() (amqp091.Queue, string, string) {
		return amqp091.Queue{}, "surr0", "rpc.direct.hub0"
	}

	return NewRouter(nil, q, nil, sibs, nil), &bytes.Buffer{}, sibs
}

func introContext(buf *bytes.Buffer) context.Context {
	return zerolog.New(buf).WithContext(context.Background())
}

// TestIntroduceWithTheSameRevisionIsSilent is the ordinary case: every peer on
// the current build must not produce a warning, or the warning becomes noise
// and stops being read.
func TestIntroduceWithTheSameRevisionIsSilent(t *testing.T) {
	r, buf, sibs := introRouter(t)

	body, err := json.Marshal(protocols.Sibling{
		Commands:         []protocols.Command{"nodes.add"},
		ProtocolRevision: protocols.ProtocolRevision,
		Version:          "abc123",
	})
	if err != nil {
		t.Fatalf("marshal sibling: %v", err)
	}

	if handleErr := r.handlerIntroCommand(introContext(buf), "rpc.direct.hub0", body); handleErr != nil {
		t.Fatalf("handle introduce: %v", handleErr)
	}

	if strings.Contains(buf.String(), "mismatch") {
		t.Errorf("logged a mismatch for a matching peer: %s", buf.String())
	}

	sibling := protocols.Sibling{}
	if ok := sibs.Get("rpc.direct.hub0", &sibling); !ok {
		t.Fatal("peer was not registered")
	}
}

// TestIntroduceWithADifferentRevisionWarnsAndStillRegisters is the mixed
// deployment case. The peer is registered anyway -- refusing it would turn a
// warning into an outage -- but the disagreement is on the record before it
// surfaces as an unexplained failure on some later command.
func TestIntroduceWithADifferentRevisionWarnsAndStillRegisters(t *testing.T) {
	r, buf, sibs := introRouter(t)

	body, err := json.Marshal(protocols.Sibling{
		Commands:         []protocols.Command{"nodes.add"},
		ProtocolRevision: protocols.ProtocolRevision + 1,
		Version:          "newer-build",
	})
	if err != nil {
		t.Fatalf("marshal sibling: %v", err)
	}

	if handleErr := r.handlerIntroCommand(introContext(buf), "rpc.direct.hub0", body); handleErr != nil {
		t.Fatalf("handle introduce: %v", handleErr)
	}

	logged := buf.String()
	for _, want := range []string{
		"sibling protocol revision mismatch",
		"newer-build",
		"rpc.direct.hub0",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("warning does not mention %q: %s", want, logged)
		}
	}

	sibling := protocols.Sibling{}
	if ok := sibs.Get("rpc.direct.hub0", &sibling); !ok {
		t.Fatal("mismatched peer was not registered")
	}
}

// TestIntroduceWithoutARevisionWarns is the rolling-upgrade case: a peer built
// before the field existed decodes as revision 0, which must warn rather than
// pass silently.
func TestIntroduceWithoutARevisionWarns(t *testing.T) {
	r, buf, _ := introRouter(t)

	if err := r.handlerIntroCommand(
		introContext(buf), "rpc.direct.hub0", []byte(`{"commands":["nodes.add"]}`),
	); err != nil {
		t.Fatalf("handle introduce: %v", err)
	}

	if !strings.Contains(buf.String(), "mismatch") {
		t.Errorf("a pre-revision peer did not warn: %s", buf.String())
	}
}

// TestIntroduceWithoutReplyToIsRejected guards the registry key: an introduce
// with no reply_to cannot be addressed, so storing it would create an entry no
// Send can ever reach.
func TestIntroduceWithoutReplyToIsRejected(t *testing.T) {
	r, buf, _ := introRouter(t)

	err := r.handlerIntroCommand(introContext(buf), "", []byte(`{"commands":[]}`))
	if err == nil {
		t.Fatal("empty reply_to was accepted")
	}
}

// TestReplyingWithoutAReplyToIsWarnedAbout pins the only trace this failure
// leaves. A request with no ReplyTo cannot be answered at all, so the requester
// times out; without the warning the answering side's log is silent about a
// reply it dropped.
func TestReplyingWithoutAReplyToIsWarnedAbout(t *testing.T) {
	r, buf, _ := introRouter(t)

	hs := protocols.NewHandlers()
	if setErr := hs.Set("test.command", func(ctx context.Context, _ uuid.UUID, _ protocols.RouterRequest, rep protocols.ReplyFunc) {
		if repErr := rep(ctx, protocols.ResultResponse([]byte(`{}`))); repErr == nil {
			t.Error("replying to a request without a ReplyTo reported success")
		}
	}); setErr != nil {
		t.Fatalf("set handler: %v", setErr)
	}

	r.gh = hs

	if err := r.handleInbound(introContext(buf), "test.command", "", uuid.New(), nil); err != nil {
		t.Fatalf("handle inbound: %v", err)
	}

	if !strings.Contains(buf.String(), "no ReplyTo") {
		t.Errorf("the dropped reply was not logged: %s", buf.String())
	}
}
