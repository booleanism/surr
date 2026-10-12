package srt

import (
	"testing"
	"time"
)

func TestPendingTakeIsOneShot(t *testing.T) {
	p := newPending[string]()

	if !p.put("peer:1", "value") {
		t.Fatal("put was refused")
	}

	got, taken := p.take("peer:1")
	if !taken || got != "value" {
		t.Fatalf("take: got (%q, %v), want (%q, true)", got, taken, "value")
	}

	if _, again := p.take("peer:1"); again {
		t.Fatal("take returned the same entry twice")
	}

	if p.len() != 0 {
		t.Fatalf("len is %d; want 0", p.len())
	}
}

// A handshake that is abandoned between the callback and Accept produces no
// Accept, so nothing else would ever remove its entry: without the sweep the
// map grows for the lifetime of the process, one entry per abandoned
// handshake.
func TestPendingSweepsEntriesWhoseHandshakeNeverCompleted(t *testing.T) {
	now := time.Unix(0, 0)

	p := newPending[string]()
	p.now = func() time.Time { return now }

	if !p.put("abandoned", "value") {
		t.Fatal("put was refused")
	}

	now = now.Add(pendingTTL + time.Second)

	if !p.put("fresh", "value") {
		t.Fatal("put was refused")
	}

	if _, ok := p.take("abandoned"); ok {
		t.Fatal("the expired entry survived the sweep")
	}

	if _, ok := p.take("fresh"); !ok {
		t.Fatal("the sweep removed the entry it had just recorded")
	}
}

// A burst inside one TTL window has to be capped too, and the refusal must not
// disturb the entries already there.
func TestPendingRefusesWhenFull(t *testing.T) {
	p := newPending[string]()
	p.max = 2

	if !p.put("a", "1") || !p.put("b", "2") {
		t.Fatal("put was refused below the cap")
	}

	if p.put("c", "3") {
		t.Fatal("put was accepted at the cap")
	}

	// The cap counts distinct peers: a re-put of a peer already present is not
	// a new entry and must still succeed, or a retried handshake would be
	// rejected because of its own earlier attempt.
	if !p.put("a", "1") {
		t.Fatal("re-put of an existing peer was refused")
	}

	if p.len() != 2 {
		t.Fatalf("len is %d; want 2", p.len())
	}
}
