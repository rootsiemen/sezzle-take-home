package openmeteo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

type trackedErrorBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *trackedErrorBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *trackedErrorBody) Close() error { b.closed = true; return nil }

func TestVendorErrorBodyBoundedBeforeReading(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		truncated        bool
	}{
		{name: "large", body: strings.Repeat("x", 1<<20) + "PRIVATE_TAIL", truncated: true},
		{name: "Unicode boundary", body: strings.Repeat("€", 100), truncated: true},
		{name: "exact boundary", body: strings.Repeat("x", 256), want: strings.Repeat("x", 256)},
		{name: "sanitized", body: " \x1bboom\r\n\x00\xff\u2028upstream ", want: "boom    upstream"},
		{name: "empty", want: "Service Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedErrorBody{Reader: strings.NewReader(tc.body)}
			client := NewClient(Config{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 503, Status: "503 PRIVATE_REASON", Body: body, Header: make(http.Header), Request: r}, nil
			})}})
			_, _, err := client.CurrentWeather(context.Background(), 1, 2)
			var vendorError *HTTPError
			if !errors.As(err, &vendorError) {
				t.Fatalf("error = %v, want HTTPError", err)
			}
			if body.read > maxErrorMessageBytes+1 || !body.closed {
				t.Fatalf("body read=%d, closed=%v", body.read, body.closed)
			}
			message := vendorError.Message
			if len(message) > maxErrorMessageBytes || !utf8.ValidString(message) || strings.ContainsFunc(message, unicode.IsControl) {
				t.Fatalf("unsafe error preview: %q", message)
			}
			if strings.HasSuffix(message, " [truncated]") != tc.truncated {
				t.Fatalf("unexpected truncation: %q", message)
			}
			if tc.want != "" && message != tc.want {
				t.Fatalf("preview = %q, want %q", message, tc.want)
			}
			if strings.Contains(err.Error(), "PRIVATE_") || !vendorError.Retryable() || vendorError.StatusCodeValue() != 503 {
				t.Fatalf("error leaked tail/reason or changed classification: %v", err)
			}
		})
	}
}

func TestHTTPErrorBoundsManuallyConstructedMessage(t *testing.T) {
	err := &HTTPError{StatusCode: 502, Message: strings.Repeat("x", 1<<20) + "PRIVATE_TAIL"}
	message := strings.TrimPrefix(err.Error(), "Open-Meteo returned HTTP 502: ")
	if len(message) > maxErrorMessageBytes || !strings.HasSuffix(message, " [truncated]") || strings.Contains(message, "PRIVATE_TAIL") {
		t.Fatalf("unsafe error: %q", message)
	}
}

func TestSearchValidatesLocationBeforeTransport(t *testing.T) {
	for _, name := range []string{strings.Repeat("x", 257), strings.Repeat("é", 129), "Paris\x00", "Paris\xff"} {
		calls := 0
		client := NewClient(Config{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("unexpected vendor call")
		})}})
		if _, err := client.Search(context.Background(), name); err == nil || calls != 0 {
			t.Fatalf("invalid name reached vendor: calls=%d, err=%v", calls, err)
		}
	}
}

func TestSearchAcceptsLocationByteBoundary(t *testing.T) {
	name := strings.Repeat("é", 128)
	calls := 0
	client := NewClient(Config{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if got := r.URL.Query().Get("name"); got != name {
			t.Fatalf("vendor name = %q", got)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"results":[{"name":"test"}]}`)), Request: r}, nil
	})}})
	if _, err := client.Search(context.Background(), name); err != nil || calls != 1 {
		t.Fatalf("valid boundary rejected: calls=%d, err=%v", calls, err)
	}
}
