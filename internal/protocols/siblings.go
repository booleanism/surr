package protocols

import (
	"sync"
)

type Sibling struct {
	Commands []Command `json:"commands"`
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
