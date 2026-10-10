package pools

import (
	"context"
)

const DefaultNGoroutinesPool = 24

type TaskFunc func()

type goroutinesPool struct {
	t chan TaskFunc
	n uint32
}

func NewGoroutinesPool(n uint32) goroutinesPool {
	return goroutinesPool{
		n: n,
		t: make(chan TaskFunc, n),
	}
}

func (p goroutinesPool) Run(ctx context.Context, t TaskFunc) error {
	select {
	case p.t <- t:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (p goroutinesPool) Listen(ctx context.Context) error {
	if p.n == 0 {
		p.n = DefaultNGoroutinesPool
	}

	for range p.n - 1 {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-p.t:
					t()
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-p.t:
			t()
		}
	}
}
