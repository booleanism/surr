package protocols

import (
	"sync"
	"time"
)

// ProtocolRevision is the wire revision of the sibling RPC protocol. Bump it
// for a change that stops two peers talking to each other: a renamed command, a
// changed request or reply payload shape, a new required field. Adding an
// optional field is not a break, because an old peer ignores what it does not
// know, so it needs no bump.
//
// Every introduce advertises it and a peer reporting a different revision is
// logged as a warning. Without that a mixed deployment is silent: the peers
// stay connected and fail later, at the first command whose payload they
// disagree about. The Python nodes mirror this number in
// nodes/lib/core/siblings.py, and the two must be changed together.
const ProtocolRevision = 1

// Sibling is a peer's introduce payload: what it can do, and which build and
// wire revision it is. The field order is not free -- govet's fieldalignment
// wants the pointer-bearing fields packed so the garbage collector scans the
// fewest leading bytes -- and JSON field order is irrelevant to both the Go and
// Python decoders, which read by key.
type Sibling struct {
	// Version is the peer's build id, carried so a mismatch can name the build
	// it is talking to. Nothing branches on it: two peers running different
	// builds is the normal case.
	Version string `json:"version,omitempty"`

	Commands []Command `json:"commands"`

	// ProtocolRevision is the peer's wire revision, compared against
	// ProtocolRevision when its introduce arrives. A peer predating this field
	// sends nothing and decodes as 0, which is correct: it really does not
	// speak revision 1.
	ProtocolRevision int `json:"protocol_revision"`
}

// SiblingTTL is how long a sibling stays resolvable after its last introduce.
//
// Every service broadcasts an introduce once a second, so a peer that has been
// silent for this long has missed fifteen in a row -- it is not slow, it is
// gone. Expiry is what stops a node that was killed without a graceful
// nodes.remove from staying resolvable for the life of the hub process: without
// it, GetName keeps naming the dead peer, the dispatch proceeds, and every
// attempt costs a full command timeout before the sink is treated as
// unavailable (TODO.md F5). With it, the same dispatch fails immediately, which
// is the answer a registry that knows the peer is gone should give.
//
// The Python nodes mirror this in nodes/lib/core/siblings.py, and the two must
// be changed together -- a Go service and a node disagreeing about when a peer
// is dead is a difference in behaviour, not in wire format.
const SiblingTTL = 15 * time.Second

type siblings struct {
	m map[SiblingName]siblingEntry

	mu sync.Mutex
}

// siblingEntry is a Sibling plus the moment it was last advertised. The
// timestamp is deliberately not part of Sibling: that struct is the wire type,
// shared with the Python nodes, and when a peer was last heard from is local
// bookkeeping no peer can be told about. The field order is not free -- govet's
// fieldalignment wants the timestamp before the pointer-bearing Sibling, so the
// garbage collector scans the fewest leading bytes.
type siblingEntry struct {
	lastSeen time.Time
	sibling  Sibling
}

func NewSiblings() *siblings {
	return &siblings{
		m: make(map[SiblingName]siblingEntry),
	}
}

// fresh reports whether an entry may still be used, deleting it when it may not.
// The caller must hold mu. Deleting is safe during a range over the map, which
// is why GetName can expire entries as it walks them.
func (s *siblings) fresh(sn SiblingName, e siblingEntry) bool {
	if time.Since(e.lastSeen) <= SiblingTTL {
		return true
	}

	delete(s.m, sn)

	return false
}

func (s *siblings) Get(sn SiblingName, ss *Sibling) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.m[sn]
	if !ok || !s.fresh(sn, e) {
		return false
	}

	(*ss) = e.sibling

	return true
}

func (s *siblings) Set(sn SiblingName, ss Sibling) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Every introduce lands here, so this is also the refresh point: a peer that
	// keeps introducing itself never expires.
	s.m[sn] = siblingEntry{sibling: ss, lastSeen: time.Now()}

	return nil
}

func (s *siblings) GetName(c Command, sn *[]SiblingName) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	(*sn) = (*sn)[:0]
	i := 0
	for k, e := range s.m {
		if !s.fresh(k, e) {
			continue
		}

		for _, cc := range e.sibling.Commands {
			if c != cc {
				continue
			}

			(*sn) = append((*sn), k)
			i++
		}
	}

	return i
}
