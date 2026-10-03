package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"weatherlookup/internal/metrics"
	"weatherlookup/internal/openmeteo"
	"weatherlookup/internal/weather"
)

type guardedTestProvider struct {
	current func(context.Context) (weather.Location, weather.CurrentWeather, error)
}

func (p guardedTestProvider) Search(context.Context, string) (weather.Location, error) {
	return weather.Location{}, nil
}
func (p guardedTestProvider) CurrentWeather(ctx context.Context, _, _ float64) (weather.Location, weather.CurrentWeather, error) {
	return p.current(ctx)
}

func TestHTTPAdmissionRejectsWithoutBlockingHealthAndReleasesOnCancellation(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int64
	p := guardedTestProvider{current: func(ctx context.Context) (weather.Location, weather.CurrentWeather, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return weather.Location{}, weather.CurrentWeather{}, ctx.Err()
		}
		return weather.Location{}, weather.CurrentWeather{}, nil
	}}
	m := metrics.New()
	s := weather.NewServiceWithOptions(p, weather.Options{Observer: m})
	h := NewHandlerWithOptions(s, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Metrics: m, MaxInFlight: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(first, httptest.NewRequest("GET", "/weather?location=London", nil).WithContext(ctx))
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	// The versioned alias must share the same admission limit.
	rejected := httptest.NewRecorder()
	h.ServeHTTP(rejected, httptest.NewRequest("GET", "/v1/weather?location=Paris", nil))
	if rejected.Code != 503 || rejected.Header().Get("Retry-After") != "1" {
		t.Fatalf("rejection: %d %v", rejected.Code, rejected.Header())
	}
	for _, path := range []string{"/healthz", "/metrics"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatalf("%s blocked by admission", path)
		}
		if path == "/metrics" {
			for _, sample := range []string{"weatherlookup_lookup_in_flight 1", "weatherlookup_lookup_max_in_flight 1", "weatherlookup_lookup_rejected_total 1"} {
				if !strings.Contains(w.Body.String(), sample+"\n") {
					t.Fatalf("missing %s", sample)
				}
			}
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request did not finish")
	}
	if first.Code != 504 {
		t.Fatalf("cancelled request status = %d", first.Code)
	}
	next := httptest.NewRecorder()
	h.ServeHTTP(next, httptest.NewRequest("GET", "/weather?location=Paris", nil))
	if next.Code != 200 || calls.Load() != 2 {
		t.Fatalf("capacity not released: status=%d calls=%d", next.Code, calls.Load())
	}
}

func TestHTTPOpenCircuitReturns503WithRetryAfter(t *testing.T) {
	var calls atomic.Int64
	p := guardedTestProvider{current: func(context.Context) (weather.Location, weather.CurrentWeather, error) {
		calls.Add(1)
		return weather.Location{}, weather.CurrentWeather{}, &openmeteo.HTTPError{StatusCode: 503}
	}}
	now := time.Now()
	s := weather.NewServiceWithOptions(p, weather.Options{MaxAttempts: 1, BreakerFailureThreshold: 1, Now: func() time.Time { return now }})
	h := NewHandler(s, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i, expected := range []int{502, 503} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/weather?latitude=1&longitude=2", nil))
		if w.Code != expected {
			t.Fatalf("request %d status = %d, want %d", i, w.Code, expected)
		}
		if i == 1 && w.Header().Get("Retry-After") != "30" {
			t.Fatalf("Retry-After = %q", w.Header().Get("Retry-After"))
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("open circuit reached vendor: %d calls", calls.Load())
	}
}

func TestVendorCapacityErrorReturns503(t *testing.T) {
	w := httptest.NewRecorder()
	writeServiceError(w, weather.ErrVendorBusy)
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("response = %d %v", w.Code, w.Header())
	}
}
