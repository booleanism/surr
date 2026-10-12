package main

import (
	"context"
	"errors"
	"testing"
)

func TestRunFailureIsSilentWhenTheShutdownWasAskedFor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := runFailure(ctx); err != nil {
		t.Fatalf("a cancelled context is a clean shutdown, got %v", err)
	}
}

func TestRunFailureIsSilentBeforeAnythingHasFailed(t *testing.T) {
	if err := runFailure(context.Background()); err != nil {
		t.Fatalf("nothing has failed yet, got %v", err)
	}
}

// The bus error is the only thing that distinguishes "the operator stopped us"
// from "the source we were told to read does not exist", so it is what decides
// the exit code.
func TestRunFailureReportsTheBusError(t *testing.T) {
	busErr := errors.New("Failed to connect")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(busErr)

	if err := runFailure(ctx); !errors.Is(err, busErr) {
		t.Fatalf("a bus error has to come back out, got %v", err)
	}
}

func TestRunFailureReportsADeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	<-ctx.Done()

	if err := runFailure(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a deadline is not a clean shutdown, got %v", err)
	}
}
