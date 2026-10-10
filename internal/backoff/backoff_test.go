package backoff

import (
	"context"
	"testing"
	"time"
)

func TestNextGrowsAndCaps(t *testing.T) {
	b := New(10*time.Millisecond, 40*time.Millisecond)

	var prev time.Duration

	for i := range 10 {
		got := b.Next()
		if got < prev {
			t.Fatalf("attempt %d: delay went backwards: %s after %s", i, got, prev)
		}

		if got > 40*time.Millisecond {
			t.Fatalf("attempt %d: delay %s exceeds the maximum", i, got)
		}

		prev = got
	}
}

func TestResetStartsOver(t *testing.T) {
	b := New(10*time.Millisecond, time.Second)

	for range 5 {
		b.Next()
	}

	b.Reset()

	if got := b.Next(); got >= time.Second {
		t.Fatalf("after Reset the first delay was %s, want the base delay", got)
	}
}

func TestNewDefaultsOnNonPositive(t *testing.T) {
	b := New(0, 0)

	got := b.Next()
	if got < DefaultBase || got > DefaultBase+DefaultBase/4 {
		t.Fatalf("first delay was %s, want %s plus at most a quarter of jitter", got, DefaultBase)
	}
}

func TestWaitReturnsOnCancellation(t *testing.T) {
	b := New(time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := b.Wait(ctx); err == nil {
		t.Fatal("Wait returned nil for a cancelled context")
	}
}
