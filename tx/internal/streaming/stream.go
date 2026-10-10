package streaming

import (
	"io"
	"sync"
)

type WriterToCloser interface {
	io.WriterTo
	io.Closer
}

type StreamDeps struct {
	OpenInRoot func(string, string) (WriterToCloser, error)
}

// Stream is the set of stream names the DASH server currently serves.
//
// It is touched from the HTTP handlers (which render the index and validate a
// request) and from the recording pipeline (which registers and removes a
// stream), so every access is guarded. It used to be a plain map plus an
// atomic counter and a `for s.c.Load() > 1 {}` spin: the map was read and
// written without a lock, and because ref() incremented the same counter the
// spin waited on, two concurrent requests livelocked at 100% CPU.
type Stream struct {
	d  map[string]struct{}
	mu sync.RWMutex
}

func NewStream() *Stream {
	return &Stream{d: make(map[string]struct{})}
}

// Names returns a snapshot of the registered names.
//
// A snapshot rather than an iterator: the caller renders a response body while
// walking it, which must not hold the lock.
func (s *Stream) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.d))
	for k := range s.d {
		names = append(names, k)
	}

	return names
}

func (s *Stream) IsExist(k string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.d[k]

	return ok
}

func (s *Stream) Set(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.d[k] = struct{}{}
}

func (s *Stream) Delete(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.d, k)
}
