package protocols

import (
	"encoding/json"
	"testing"
	"time"
)

// TestSiblingWireShape pins the introduce payload. Both the Go services and the
// Python nodes encode and decode this struct, so a field renamed on one side
// silently stops being read on the other.
func TestSiblingWireShape(t *testing.T) {
	body, err := json.Marshal(Sibling{
		Commands:         []Command{"nodes.add"},
		ProtocolRevision: ProtocolRevision,
		Version:          "abc123",
	})
	if err != nil {
		t.Fatalf("marshal sibling: %v", err)
	}

	var decoded map[string]any
	if unmarshalErr := json.Unmarshal(body, &decoded); unmarshalErr != nil {
		t.Fatalf("unmarshal sibling %s: %v", body, unmarshalErr)
	}

	if got := decoded["protocol_revision"]; got != float64(ProtocolRevision) {
		t.Errorf("protocol_revision is %v, want %d", got, ProtocolRevision)
	}

	if got := decoded["version"]; got != "abc123" {
		t.Errorf("version is %v, want %q", got, "abc123")
	}
}

// TestSiblingWithoutRevisionDecodesAsZero is the rolling-upgrade case: a peer
// built before the revision field existed sends no protocol_revision key. It
// must decode as 0 rather than failing the decode, because 0 is the truthful
// answer -- that peer really does not speak the current revision -- and it is
// what makes the mismatch warning fire.
func TestSiblingWithoutRevisionDecodesAsZero(t *testing.T) {
	sibling := Sibling{}
	if err := json.Unmarshal([]byte(`{"commands":["nodes.add"]}`), &sibling); err != nil {
		t.Fatalf("unmarshal pre-revision body: %v", err)
	}

	if sibling.ProtocolRevision != 0 {
		t.Errorf("protocol revision is %d, want 0", sibling.ProtocolRevision)
	}

	if len(sibling.Commands) != 1 || sibling.Commands[0] != "nodes.add" {
		t.Errorf("commands are %v, want [nodes.add]", sibling.Commands)
	}
}

// TestProtocolRevisionIsUsable guards the constant against being left at zero,
// which would make every peer compare equal and the mismatch warning dead code.
func TestProtocolRevisionIsUsable(t *testing.T) {
	if ProtocolRevision < 1 {
		t.Fatalf("ProtocolRevision is %d, want >= 1", ProtocolRevision)
	}
}

// aged returns a registry holding one entry for sn whose last introduce was long
// enough ago that it must be treated as dead. Building the entry directly is
// what keeps the expiry tests from sleeping for the whole TTL.
func aged(sibs *siblings, sn SiblingName, sibling Sibling) {
	sibs.m[sn] = siblingEntry{sibling: sibling, lastSeen: time.Now().Add(-2 * SiblingTTL)}
}

// TestASiblingThatStoppedIntroducingExpires is TODO.md F5: introduce registers a
// peer and, before this, nothing ever removed it, so a node killed without a
// graceful nodes.remove stayed resolvable for the life of the process. GetName
// then named the dead peer, the dispatch proceeded, and every attempt cost a
// full command timeout before the sink was treated as unavailable.
func TestASiblingThatStoppedIntroducingExpires(t *testing.T) {
	sibs := NewSiblings()
	aged(sibs, "rpc.direct.node0", Sibling{Commands: []Command{"n0.sink.start"}})

	var names []SiblingName
	if n := sibs.GetName("n0.sink.start", &names); n != 0 {
		t.Fatalf("GetName returned %d names %v for a peer that stopped introducing, want 0", n, names)
	}

	var sibling Sibling
	if ok := sibs.Get("rpc.direct.node0", &sibling); ok {
		t.Error("Get still resolves a peer that stopped introducing")
	}

	// The entry is dropped, not merely hidden: a registry that kept it would grow
	// one dead key per node for the life of the process.
	if len(sibs.m) != 0 {
		t.Errorf("registry still holds %d entries after expiry, want 0", len(sibs.m))
	}
}

// TestAFreshSiblingIsStillResolvable is the other half: the TTL must not make a
// live peer unreachable, which would break every dispatch in the system.
func TestAFreshSiblingIsStillResolvable(t *testing.T) {
	sibs := NewSiblings()
	if err := sibs.Set("rpc.direct.node0", Sibling{Commands: []Command{"n0.sink.start"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var names []SiblingName
	if n := sibs.GetName("n0.sink.start", &names); n != 1 || names[0] != "rpc.direct.node0" {
		t.Fatalf("GetName returned %d names %v, want 1 [rpc.direct.node0]", n, names)
	}

	var sibling Sibling
	if ok := sibs.Get("rpc.direct.node0", &sibling); !ok {
		t.Fatal("Get does not resolve a peer that just introduced itself")
	}
}

// TestReIntroducingRefreshesTheDeadline pins that the TTL is measured from the
// last introduce, not from the first. A long-running node is exactly the case
// that would break if Set did not stamp a new time.
func TestReIntroducingRefreshesTheDeadline(t *testing.T) {
	sibs := NewSiblings()
	aged(sibs, "rpc.direct.node0", Sibling{Commands: []Command{"n0.sink.start"}})

	// The peer introduces itself again, the way the one-second ticker does.
	if err := sibs.Set("rpc.direct.node0", Sibling{Commands: []Command{"n0.sink.start"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var names []SiblingName
	if n := sibs.GetName("n0.sink.start", &names); n != 1 {
		t.Fatalf("GetName returned %d names %v after re-introduce, want 1", n, names)
	}
}

// TestExpiryIsPerSibling pins that one dead peer does not take a live one with
// it: the registry is keyed per sibling and each carries its own timestamp.
func TestExpiryIsPerSibling(t *testing.T) {
	sibs := NewSiblings()
	aged(sibs, "rpc.direct.dead", Sibling{Commands: []Command{"sink.start"}})
	if err := sibs.Set("rpc.direct.alive", Sibling{Commands: []Command{"sink.start"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var names []SiblingName
	if n := sibs.GetName("sink.start", &names); n != 1 || names[0] != "rpc.direct.alive" {
		t.Fatalf("GetName returned %d names %v, want 1 [rpc.direct.alive]", n, names)
	}
}
