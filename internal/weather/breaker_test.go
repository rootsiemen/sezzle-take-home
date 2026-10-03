package weather

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitRecoveryAllowsOneProbeAndIgnoresOldCompletions(t *testing.T) {
	now := time.Now()
	b := newCircuitBreaker("forecast.current", 2, 30*time.Second, func() time.Time { return now }, nil)
	old, err := b.allow()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		generation, err := b.allow()
		if err != nil {
			t.Fatal(err)
		}
		b.complete(generation, classifiedError(true))
	}
	if b.state != circuitOpen {
		t.Fatal("circuit did not trip at threshold")
	}
	b.complete(old, nil)
	if b.state != circuitOpen {
		t.Fatal("late success incorrectly closed circuit")
	}
	_, err = b.allow()
	var open *CircuitOpenError
	if !errors.As(err, &open) || open.RetryAfter != 30*time.Second {
		t.Fatalf("open rejection = %v", err)
	}
	now = now.Add(30 * time.Second)
	var wg sync.WaitGroup
	allowed := make(chan uint64, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			generation, err := b.allow()
			if err == nil {
				allowed <- generation
			} else if !errors.Is(err, ErrCircuitOpen) {
				t.Errorf("unexpected rejection %v", err)
			}
		}()
	}
	wg.Wait()
	close(allowed)
	if len(allowed) != 1 || b.state != circuitHalfOpen {
		t.Fatalf("half-open permits = %d, state = %d", len(allowed), b.state)
	}
	probe := <-allowed
	b.complete(probe, classifiedError(true))
	if b.state != circuitOpen || b.openUntil != now.Add(30*time.Second) {
		t.Fatal("failed probe did not restart cooldown")
	}
	now = now.Add(30 * time.Second)
	probe, err = b.allow()
	if err != nil {
		t.Fatal(err)
	}
	b.complete(probe, nil)
	if b.state != circuitClosed || b.failures != 0 {
		t.Fatal("successful probe did not close/reset circuit")
	}
	b.complete(old, classifiedError(true))
	if b.state != circuitClosed || b.failures != 0 {
		t.Fatal("old failure contaminated recovered circuit")
	}
}

func TestCircuitCancellationAndPermanentResponses(t *testing.T) {
	now := time.Now()
	b := newCircuitBreaker("geocoding.search", 2, time.Second, func() time.Time { return now }, nil)
	for _, err := range []error{classifiedError(true), context.Canceled, classifiedError(false)} {
		generation, _ := b.allow()
		b.complete(generation, err)
	}
	if b.state != circuitClosed || b.failures != 0 {
		t.Fatal("non-transient response did not reset failures")
	}
	for i := 0; i < 2; i++ {
		generation, _ := b.allow()
		b.complete(generation, context.DeadlineExceeded)
	}
	now = now.Add(time.Second)
	probe, err := b.allow()
	if err != nil {
		t.Fatal(err)
	}
	b.complete(probe, context.Canceled)
	if b.state != circuitHalfOpen {
		t.Fatal("cancelled probe changed availability state")
	}
	probe, err = b.allow()
	if err != nil {
		t.Fatal("cancelled probe stranded half-open circuit")
	}
	b.complete(probe, classifiedError(false))
	if b.state != circuitClosed {
		t.Fatal("non-transient probe did not close circuit")
	}
}
