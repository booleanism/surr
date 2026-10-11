package protocols

import (
	"encoding/json"
	"testing"
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
