package openmeteo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"weatherlookup/internal/weather"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestVendorStatusAndDecodeRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		attempts int
	}{
		{"rate_limited", 429, "rate limited", 2},
		{"server_error", 500, "error", 2},
		{"bad_gateway", 502, "error", 2},
		{"unavailable", 503, "error", 2},
		{"gateway_timeout", 504, "error", 2},
		{"bad_request", 400, "error", 1},
		{"unauthorized", 401, "error", 1},
		{"not_found", 404, "error", 1},
		{"not_implemented", 501, "error", 1},
		{"version_unsupported", 505, "error", 1},
		{"malformed_json", 200, "not JSON", 1},
		{"truncated_json", 200, `{"current":`, 1},
		{"empty_json", 200, "", 1},
		{"missing_current", 200, `{}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := NewClient(Config{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: tc.status, Status: fmt.Sprint(tc.status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})}})
			s := weather.NewServiceWithOptions(client, weather.Options{MaxAttempts: 2, RetryDelay: func(int) time.Duration { return 0 }})
			lat, lon := 1.0, 2.0
			if _, err := s.Lookup(context.Background(), weather.Query{Latitude: &lat, Longitude: &lon}); err == nil {
				t.Fatal("expected vendor failure")
			}
			if calls != tc.attempts {
				t.Fatalf("calls = %d, want %d", calls, tc.attempts)
			}
		})
	}
}
