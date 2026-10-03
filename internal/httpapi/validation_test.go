package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"weatherlookup/internal/cache"
	"weatherlookup/internal/openmeteo"
	"weatherlookup/internal/weather"
)

type validationProvider struct {
	calls int
	err   error
}

func (p *validationProvider) Search(context.Context, string) (weather.Location, error) {
	p.calls++
	return weather.Location{}, p.err
}

func (p *validationProvider) CurrentWeather(context.Context, float64, float64) (weather.Location, weather.CurrentWeather, error) {
	p.calls++
	return weather.Location{}, weather.CurrentWeather{}, p.err
}

func TestInvalidLocationNotForwardedCachedOrPersisted(t *testing.T) {
	for _, path := range []string{"/weather", "/v1/weather"} {
		for _, input := range []string{strings.Repeat("x", 257), strings.Repeat("é", 129), strings.Repeat(" ", 256) + "Paris", "Paris\x00", "Paris\xff"} {
			provider := &validationProvider{}
			cached := cache.New[weather.Result](10, time.Minute)
			sink := &recordingSink{}
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			handler := NewHandlerWithOptions(weather.NewServiceWithOptions(provider, weather.Options{Cache: cached}), time.Second, logger, Options{ResponseSink: sink})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest("GET", path+"?location="+url.QueryEscape(input), nil))
			if recorder.Code != 400 || provider.calls != 0 || cached.Stats().Entries != 0 {
				t.Fatalf("validation failed: status=%d, calls=%d, cache=%+v", recorder.Code, provider.calls, cached.Stats())
			}
			if len(sink.records) != 1 {
				t.Fatalf("response records=%d", len(sink.records))
			}
			record := sink.records[0]
			if record.Location != "" || record.QueryMode != "location" || record.Status != 400 || record.Error == "" {
				t.Fatalf("invalid location retained or rejection not recorded: %+v", record)
			}
			encodedInput, _ := json.Marshal(input)
			if bytes.Contains(record.Response, encodedInput) || bytes.Contains(logs.Bytes(), encodedInput) {
				t.Fatal("invalid input reflected into response/logs")
			}
		}
	}
}

func TestValidLocationBoundaryPersisted(t *testing.T) {
	input := strings.Repeat("é", 128)
	sink := &recordingSink{}
	h := NewHandlerWithOptions(weather.NewService(fakeProvider{}), time.Second, nil, Options{ResponseSink: sink})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/weather?location="+url.QueryEscape(input), nil))
	if w.Code != 200 || len(sink.records) != 1 || sink.records[0].Location != input {
		t.Fatalf("valid boundary rejected: status=%d, records=%+v", w.Code, sink.records)
	}
}

func TestLookupErrorLogLevelAndBodyLimit(t *testing.T) {
	for _, tc := range []struct {
		name, level string
		err         error
		status      int
	}{
		{"not found", "INFO", openmeteo.ErrLocationNotFound, 404},
		{"wrapped not found", "INFO", fmt.Errorf("geocoding: %w", openmeteo.ErrLocationNotFound), 404},
		{"vendor 404 is an upstream failure", "ERROR", &openmeteo.HTTPError{StatusCode: 404, Message: "upstream route missing"}, 502},
		{"large upstream failure", "ERROR", &openmeteo.HTTPError{StatusCode: 503, Message: strings.Repeat("x", 1<<20) + "PRIVATE_TAIL"}, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			s := weather.NewServiceWithOptions(&validationProvider{err: tc.err}, weather.Options{MaxAttempts: 1})
			h := NewHandler(s, time.Second, logger)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/weather?location=Paris", nil))
			if w.Code != tc.status {
				t.Fatalf("status=%d, want %d", w.Code, tc.status)
			}
			var entry struct {
				Level string `json:"level"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(&logs).Decode(&entry); err != nil {
				t.Fatal(err)
			}
			if entry.Level != tc.level || entry.Error == "" || len(entry.Error) > 300 || strings.Contains(entry.Error, "PRIVATE_TAIL") {
				t.Fatalf("unexpected error log: %+v", entry)
			}
			if strings.Contains(w.Body.String(), "Open-Meteo") || strings.Contains(w.Body.String(), "PRIVATE_TAIL") {
				t.Fatal("vendor diagnostics exposed to client")
			}
		})
	}
}
