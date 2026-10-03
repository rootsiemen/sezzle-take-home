package weather

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"weatherlookup/internal/cache"
)

type classifiedError bool

func (e classifiedError) Error() string   { return "classified vendor error" }
func (e classifiedError) Retryable() bool { return bool(e) }

func TestRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts int
	}{
		{"unknown", errors.New("bad configuration"), 1},
		{"malformed_json", &json.SyntaxError{}, 1},
		{"truncated_json", io.ErrUnexpectedEOF, 1},
		{"permanent", classifiedError(false), 1},
		{"cancelled", context.Canceled, 1},
		{"transient", classifiedError(true), 3},
		{"deadline", context.DeadlineExceeded, 3},
		{"connection_reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, 3},
		{"connection_refused", fmt.Errorf("connect: %w", syscall.ECONNREFUSED), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			_, err := retry(context.Background(), "test", 3, func(int) time.Duration { return 0 }, time.Now, nil, func(context.Context) (int, error) {
				calls++
				return 0, tc.err
			})
			if !errors.Is(err, tc.err) || calls != tc.attempts {
				t.Fatalf("error/calls = %v/%d", err, calls)
			}
		})
	}
}

type controlledProvider struct {
	current func(context.Context) (Location, CurrentWeather, error)
}

func (p controlledProvider) Search(context.Context, string) (Location, error) { return Location{}, nil }
func (p controlledProvider) CurrentWeather(ctx context.Context, _, _ float64) (Location, CurrentWeather, error) {
	return p.current(ctx)
}

func TestStalePolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		age, elapsed time.Duration
		err          error
		stale        bool
	}{
		{"within_limit", 6 * time.Minute, 0, classifiedError(true), true},
		{"at_limit", 15 * time.Minute, 0, classifiedError(true), true},
		{"past_limit", 15*time.Minute + time.Nanosecond, 0, classifiedError(true), false},
		{"ages_out_during_vendor_call", 14 * time.Minute, 2 * time.Minute, classifiedError(true), false},
		{"permanent_failure", 6 * time.Minute, 0, classifiedError(false), false},
		{"unknown_failure", 6 * time.Minute, 0, errors.New("invalid response"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var elapsed atomic.Int64
			clock := func() time.Time { return time.Unix(0, elapsed.Load()) }
			c := cache.NewWithClock[Result](2, 5*time.Minute, clock)
			lat, lon := 1.0, 2.0
			q := Query{Latitude: &lat, Longitude: &lon}
			c.Set(cacheKey(q), Result{Current: CurrentWeather{TemperatureC: 20}})
			elapsed.Store(int64(tc.age))
			p := controlledProvider{current: func(context.Context) (Location, CurrentWeather, error) {
				elapsed.Add(int64(tc.elapsed))
				return Location{}, CurrentWeather{}, tc.err
			}}
			s := NewServiceWithOptions(p, Options{Cache: c, MaxAttempts: 1, MaxStaleAge: 15 * time.Minute})
			_, metadata, err := s.LookupWithMetadata(context.Background(), q)
			if tc.stale {
				if err != nil || metadata.CacheStatus != CacheStale || metadata.Age != tc.age+tc.elapsed {
					t.Fatalf("metadata/error = %+v/%v", metadata, err)
				}
			} else if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
		})
	}
}

// Done is evaluated by the caller's select after it has joined DoChan.
// This handshake avoids sleeps/scheduler assumptions in cancellation tests.
type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}

func TestSharedFillCallersCancelIndependently(t *testing.T) {
	for _, cancelLeader := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel_leader_%t", cancelLeader), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			defer finish()
			p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				select {
				case <-release:
					return Location{}, CurrentWeather{TemperatureC: 21}, nil
				case <-ctx.Done():
					return Location{}, CurrentWeather{}, ctx.Err()
				}
			}}
			s := NewServiceWithOptions(p, Options{Cache: cache.New[Result](2, time.Minute), FillTimeout: time.Second})
			lat, lon := 1.0, 2.0
			q := Query{Latitude: &lat, Longitude: &lon}
			leaderContext, cancelFirst := context.WithCancel(context.Background())
			defer cancelFirst()
			waiterContext, cancelSecond := context.WithCancel(context.Background())
			defer cancelSecond()
			waiter := &waitingContext{Context: waiterContext, waiting: make(chan struct{})}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { _, err := s.Lookup(leaderContext, q); first <- err }()
			await(t, started)
			go func() { _, err := s.Lookup(waiter, q); second <- err }()
			await(t, waiter.waiting)
			cancelled, survivor := second, first
			if cancelLeader {
				cancelFirst()
				cancelled, survivor = first, second
			} else {
				cancelSecond()
			}
			if err := await(t, cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled caller: %v", err)
			}
			finish()
			if err := await(t, survivor); err != nil {
				t.Fatalf("survivor: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("vendor calls = %d, want 1", calls.Load())
			}
			_, metadata, err := s.LookupWithMetadata(context.Background(), q)
			if err != nil || metadata.CacheStatus != CacheHit {
				t.Fatalf("fill did not populate cache: %+v %v", metadata, err)
			}
		})
	}
}

func TestAbandonedSharedFillHasItsOwnDeadline(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan error, 1)
	p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
		close(started)
		<-ctx.Done()
		stopped <- ctx.Err()
		return Location{}, CurrentWeather{}, ctx.Err()
	}}
	s := NewServiceWithOptions(p, Options{Cache: cache.New[Result](2, time.Minute), FillTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := s.Lookup(ctx, Query{Location: "London"}); finished <- err }()
	await(t, started)
	cancel()
	if err := await(t, finished); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller error = %v", err)
	}
	if err := await(t, stopped); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fill error = %v", err)
	}
}

func TestCancelledContextDoesNotCallVendorOrReturnCachedData(t *testing.T) {
	p := &fakeProvider{}
	c := cache.New[Result](2, time.Minute)
	q := Query{Location: "London"}
	c.Set(cacheKey(q), Result{})
	s := NewServiceWithOptions(p, Options{Cache: c})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Lookup(ctx, q); !errors.Is(err, context.Canceled) || p.lookups != 0 {
		t.Fatalf("error/calls = %v/%d", err, p.lookups)
	}
}

type staleObserver struct{ count atomic.Int64 }

func (*staleObserver) CacheLookup(string)                          {}
func (*staleObserver) CacheStats(int, uint64)                      {}
func (*staleObserver) VendorRequest(string, string, time.Duration) {}
func (*staleObserver) VendorRetry(string)                          {}
func (o *staleObserver) StaleResponse()                            { o.count.Add(1) }

func TestConcurrentMissesShareOneFillAndCountEachStaleResponse(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("vendor_fails_%t", fail), func(t *testing.T) {
			var calls atomic.Int64
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			defer finish()
			p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
				calls.Add(1)
				select {
				case <-release:
				case <-ctx.Done():
					return Location{}, CurrentWeather{}, ctx.Err()
				}
				if fail {
					return Location{}, CurrentWeather{}, classifiedError(true)
				}
				return Location{}, CurrentWeather{}, nil
			}}
			var elapsed atomic.Int64
			c := cache.NewWithClock[Result](2, time.Minute, func() time.Time { return time.Unix(0, elapsed.Load()) })
			q := Query{Location: "London"}
			if fail {
				c.Set(cacheKey(q), Result{})
				elapsed.Store(int64(2 * time.Minute))
			}
			observer := &staleObserver{}
			s := NewServiceWithOptions(p, Options{Cache: c, MaxAttempts: 1, Observer: observer})
			const callers = 32
			results := make(chan error, callers)
			for i := 0; i < callers; i++ {
				ctx := &waitingContext{Context: context.Background(), waiting: make(chan struct{})}
				go func() { _, err := s.Lookup(ctx, q); results <- err }()
				await(t, ctx.waiting)
			}
			finish()
			for i := 0; i < callers; i++ {
				if err := await(t, results); err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("vendor calls = %d, want 1", calls.Load())
			}
			want := int64(0)
			if fail {
				want = callers
			}
			if observer.count.Load() != want {
				t.Fatalf("stale responses = %d, want %d", observer.count.Load(), want)
			}
		})
	}
}

func TestWaitingCallerDeadlineDoesNotCancelSharedFill(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
		close(started)
		select {
		case <-release:
			return Location{}, CurrentWeather{}, nil
		case <-ctx.Done():
			return Location{}, CurrentWeather{}, ctx.Err()
		}
	}}
	s := NewServiceWithOptions(p, Options{Cache: cache.New[Result](2, time.Minute), FillTimeout: time.Second})
	q := Query{Location: "London"}
	first := make(chan error, 1)
	go func() { _, err := s.Lookup(context.Background(), q); first <- err }()
	await(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := s.Lookup(ctx, q); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting caller error = %v", err)
	}
	finish()
	if err := await(t, first); err != nil {
		t.Fatalf("surviving caller error = %v", err)
	}
}
