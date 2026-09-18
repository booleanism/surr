package streaming

import (
	"io"
	"iter"
	"sync/atomic"
)

type WriterToCloser interface {
	io.WriterTo
	io.Closer
}

type StreamDeps struct {
	OpenInRoot func(string, string) (WriterToCloser, error)
}

type Stream struct {
	d map[string]struct{}
	c atomic.Int32
}

func (s *Stream) Iter() iter.Seq2[string, struct{}] {
	return func(yield func(string, struct{}) bool) {
		s.block()
		s.c.Add(1)
		defer s.c.Add(-1)
		for k, v := range s.d {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (s *Stream) ref() *Stream {
	s.c.Add(1)
	return s
}

func (s *Stream) Unref() {
	s.c.Add(-1)
}

func (s *Stream) IsExist(k string) bool {
	s.block()
	_, ok := s.d[k]
	return ok
}

func (s *Stream) Set(k string, v struct{}) {
	s.block()
	s.d[k] = v
}

func (s *Stream) Delete(k string) {
	s.block()
	delete(s.d, k)
}

func (s *Stream) block() {
	for s.c.Load() > 1 {
		_ = s
	}
}
