package protocols

import (
	"sync"
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

type siblings struct {
	m map[SiblingName]Sibling

	mu sync.Mutex
}

func NewSiblings() *siblings {
	return &siblings{
		m: make(map[SiblingName]Sibling),
	}
}

func (s *siblings) Get(sn SiblingName, ss *Sibling) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	(*ss), ok = s.m[sn]
	return ok
}

func (s *siblings) Set(sn SiblingName, ss Sibling) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[sn] = ss
	return nil
}

func (s *siblings) GetName(c Command, sn *[]SiblingName) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	(*sn) = (*sn)[:0]
	i := 0
	for k, v := range s.m {
		for _, cc := range v.Commands {
			if c != cc {
				continue
			}

			(*sn) = append((*sn), k)
			i++
		}
	}

	return i
}
