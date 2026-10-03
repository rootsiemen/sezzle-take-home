package weather

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"weatherlookup/internal/cache"
	"weatherlookup/internal/metrics"
)

func assertGuardMetrics(t *testing.T, m *metrics.Metrics, samples ...string) {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, sample := range samples {
		if !strings.Contains(w.Body.String(), sample+"\n") {
			t.Fatalf("missing metric %s:\n%s", sample, w.Body.String())
		}
	}
}

func TestCircuitStopsRetriesWithoutCountingRejectedVendorCalls(t *testing.T) {
	m := metrics.New()
	p := &fakeProvider{err: classifiedError(true)}
	s := NewServiceWithOptions(p, Options{MaxAttempts: 10, BreakerFailureThreshold: 2, Observer: m, RetryDelay: func(int) time.Duration { return 0 }})
	lat, lon := 1.0, 2.0
	_, err := s.Lookup(context.Background(), Query{Latitude: &lat, Longitude: &lon})
	if !errors.Is(err, ErrCircuitOpen) || p.lookups != 2 {
		t.Fatalf("error/calls = %v/%d", err, p.lookups)
	}
	assertGuardMetrics(t, m,
		`weatherlookup_vendor_requests_total{operation="forecast.current",outcome="error"} 2`,
		`weatherlookup_vendor_retries_total{operation="forecast.current"} 1`,
		`weatherlookup_circuit_state{operation="forecast.current"} 1`,
		`weatherlookup_circuit_rejections_total{operation="forecast.current"} 1`,
		`weatherlookup_vendor_in_flight 0`)
}

func TestGeocodingCircuitDoesNotBlockCoordinates(t *testing.T) {
	p := &fakeProvider{err: classifiedError(true)}
	s := NewServiceWithOptions(p, Options{MaxAttempts: 1, BreakerFailureThreshold: 1})
	if _, err := s.Lookup(context.Background(), Query{Location: "London"}); err == nil {
		t.Fatal("expected geocoding failure")
	}
	p.err = nil
	if _, err := s.Lookup(context.Background(), Query{Location: "Paris"}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("geocoding error = %v", err)
	}
	lat, lon := 1.0, 2.0
	if _, err := s.Lookup(context.Background(), Query{Latitude: &lat, Longitude: &lon}); err != nil {
		t.Fatalf("coordinates blocked by unrelated breaker: %v", err)
	}
	if p.searches != 1 || p.lookups != 1 {
		t.Fatalf("calls = %d/%d", p.searches, p.lookups)
	}
}

func TestVendorLimitBoundsUniqueLookupsWithAndWithoutCache(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache_%t", cached), func(t *testing.T) {
			started, release := make(chan struct{}, 2), make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			defer finish()
			var calls atomic.Int64
			p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
				calls.Add(1)
				started <- struct{}{}
				select {
				case <-release:
					return Location{}, CurrentWeather{}, nil
				case <-ctx.Done():
					return Location{}, CurrentWeather{}, ctx.Err()
				}
			}}
			m := metrics.New()
			options := Options{MaxVendorInFlight: 2, Observer: m}
			if cached {
				options.Cache = cache.New[Result](10, time.Minute)
			}
			s := NewServiceWithOptions(p, options)
			results := make(chan error, 2)
			for _, city := range []string{"London", "Paris"} {
				go func() { _, err := s.Lookup(context.Background(), Query{Location: city}); results <- err }()
			}
			await(t, started)
			await(t, started)
			for i := 0; i < 100; i++ {
				if _, err := s.Lookup(context.Background(), Query{Location: fmt.Sprintf("unique-%d", i)}); !errors.Is(err, ErrVendorBusy) {
					t.Fatalf("overflow lookup error = %v", err)
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("unique flood reached vendor: calls=%d", calls.Load())
			}
			assertGuardMetrics(t, m, "weatherlookup_vendor_in_flight 2", "weatherlookup_vendor_max_in_flight 2", "weatherlookup_vendor_admission_rejected_total 100")
			finish()
			for i := 0; i < 2; i++ {
				if err := await(t, results); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Lookup(context.Background(), Query{Location: "Berlin"}); err != nil {
				t.Fatalf("permit not released: %v", err)
			}
			assertGuardMetrics(t, m, "weatherlookup_vendor_in_flight 0")
		})
	}
}

func TestDetachedFillRetainsVendorPermitAfterCallerCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	var calls atomic.Int64
	p := controlledProvider{current: func(ctx context.Context) (Location, CurrentWeather, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return Location{}, CurrentWeather{}, nil
		case <-ctx.Done():
			return Location{}, CurrentWeather{}, ctx.Err()
		}
	}}
	s := NewServiceWithOptions(p, Options{Cache: cache.New[Result](10, time.Minute), MaxVendorInFlight: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := s.Lookup(ctx, Query{Location: "London"}); first <- err }()
	await(t, started)
	cancel()
	if err := await(t, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller: %v", err)
	}
	if _, err := s.Lookup(context.Background(), Query{Location: "Paris"}); !errors.Is(err, ErrVendorBusy) {
		t.Fatalf("abandoned fill released capacity early: %v", err)
	}
	waiter := &waitingContext{Context: context.Background(), waiting: make(chan struct{})}
	go func() { _, err := s.Lookup(waiter, Query{Location: "London"}); first <- err }()
	await(t, waiter.waiting)
	finish()
	if err := await(t, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(context.Background(), Query{Location: "Paris"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestCircuitOpenStillServesFreshAndBoundedStaleCache(t *testing.T) {
	var clock atomic.Int64
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	c := cache.NewWithClock[Result](10, time.Minute, now)
	p := &fakeProvider{err: classifiedError(true)}
	s := NewServiceWithOptions(p, Options{Cache: c, MaxAttempts: 1, BreakerFailureThreshold: 1, BreakerCooldown: time.Hour, Now: now})
	lat, lon := 1.0, 2.0
	q := Query{Latitude: &lat, Longitude: &lon}
	if _, err := s.Lookup(context.Background(), q); err == nil {
		t.Fatal("expected failure to trip breaker")
	}
	c.Set(cacheKey(q), Result{})
	if _, metadata, err := s.LookupWithMetadata(context.Background(), q); err != nil || metadata.CacheStatus != CacheHit {
		t.Fatalf("fresh cache blocked: %+v %v", metadata, err)
	}
	clock.Store(int64(2 * time.Minute))
	if _, metadata, err := s.LookupWithMetadata(context.Background(), q); err != nil || metadata.CacheStatus != CacheStale {
		t.Fatalf("stale cache blocked: %+v %v", metadata, err)
	}
	clock.Store(int64(16 * time.Minute))
	if _, _, err := s.LookupWithMetadata(context.Background(), q); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("over-age cache served: %v", err)
	}
	if p.lookups != 1 {
		t.Fatalf("open circuit called vendor %d times", p.lookups)
	}
}

func TestVendorCapacityStillServesFreshAndBoundedStaleCache(t *testing.T) {
	var clock atomic.Int64
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	c := cache.NewWithClock[Result](10, time.Minute, now)
	p := &fakeProvider{}
	s := NewServiceWithOptions(p, Options{Cache: c, MaxVendorInFlight: 1})
	q := Query{Location: "London"}
	c.Set(cacheKey(q), Result{})
	if !s.vendorSlots.TryAcquire() {
		t.Fatal("could not occupy vendor capacity")
	}
	defer s.vendorSlots.Release()
	if _, metadata, err := s.LookupWithMetadata(context.Background(), q); err != nil || metadata.CacheStatus != CacheHit {
		t.Fatalf("fresh cache blocked: %+v %v", metadata, err)
	}
	clock.Store(int64(2 * time.Minute))
	if _, metadata, err := s.LookupWithMetadata(context.Background(), q); err != nil || metadata.CacheStatus != CacheStale {
		t.Fatalf("stale cache blocked: %+v %v", metadata, err)
	}
	clock.Store(int64(16 * time.Minute))
	if _, _, err := s.LookupWithMetadata(context.Background(), q); !errors.Is(err, ErrVendorBusy) {
		t.Fatalf("over-age cache served: %v", err)
	}
	if p.lookups != 0 || p.searches != 0 {
		t.Fatal("overloaded lookup reached provider")
	}
}
