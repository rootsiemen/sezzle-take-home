package weather

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrVendorBusy  = errors.New("vendor lookup capacity exhausted")
	ErrCircuitOpen = errors.New("vendor circuit is open")
)

type CircuitOpenError struct{ RetryAfter time.Duration }

func (*CircuitOpenError) Error() string { return ErrCircuitOpen.Error() }
func (*CircuitOpenError) Unwrap() error { return ErrCircuitOpen }

// GuardObserver is optional so existing service observers remain compatible.
// Callbacks must not reenter the service or its guards.
type GuardObserver interface {
	VendorAdmission(active, maximum int)
	VendorRejected()
	CircuitState(operation string, state int)
	CircuitRejected(operation string)
}

const (
	circuitClosed = iota
	circuitOpen
	circuitHalfOpen
)

// Each endpoint has its own breaker. A generation prevents late completions
// from earlier in-flight attempts from closing a newly opened circuit.
type circuitBreaker struct {
	mu                         sync.Mutex
	operation                  string
	state, failures, threshold int
	generation                 uint64
	probeInFlight              bool
	openUntil                  time.Time
	cooldown                   time.Duration
	now                        func() time.Time
	observer                   GuardObserver
}

func newCircuitBreaker(operation string, threshold int, cooldown time.Duration, now func() time.Time, observer GuardObserver) *circuitBreaker {
	b := &circuitBreaker{operation: operation, threshold: threshold, cooldown: cooldown, now: now, observer: observer}
	if observer != nil {
		observer.CircuitState(operation, circuitClosed)
	}
	return b
}

func (b *circuitBreaker) allow() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == circuitOpen {
		if remaining := b.openUntil.Sub(b.now()); remaining > 0 {
			return 0, b.reject(remaining)
		}
		b.transition(circuitHalfOpen)
	}
	if b.state == circuitHalfOpen {
		if b.probeInFlight {
			return 0, b.reject(time.Second)
		}
		b.probeInFlight = true
	}
	return b.generation, nil
}

func (b *circuitBreaker) reject(after time.Duration) error {
	if b.observer != nil {
		b.observer.CircuitRejected(b.operation)
	}
	return &CircuitOpenError{RetryAfter: after}
}

func (b *circuitBreaker) complete(generation uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.generation {
		return
	}
	if errors.Is(err, context.Canceled) {
		// A disconnected caller is not evidence of a vendor outage. Release a
		// cancelled half-open probe so another caller can test recovery.
		b.probeInFlight = false
		return
	}
	if retryable(err) {
		b.failures++
		if b.state == circuitHalfOpen || b.failures >= b.threshold {
			b.openUntil = b.now().Add(b.cooldown)
			b.transition(circuitOpen)
		}
		return
	}
	// Success or a non-transient response (e.g. location not found) resets the
	// consecutive availability-failure count. Those errors aren't retried.
	b.failures = 0
	if b.state == circuitHalfOpen {
		b.transition(circuitClosed)
	}
}

func (b *circuitBreaker) transition(state int) {
	b.state = state
	b.generation++
	b.probeInFlight = false
	if b.observer != nil {
		b.observer.CircuitState(b.operation, state)
	}
}

func throughCircuit[T any](ctx context.Context, breaker *circuitBreaker, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	generation, err := breaker.allow()
	if err != nil {
		return zero, err
	}
	value, err := call(ctx)
	breaker.complete(generation, err)
	return value, err
}
