package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsMiddlewareAndHandler(t *testing.T) {
	metricSet := New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /weather", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("GET /metrics", metricSet)
	handler := metricSet.Middleware(mux)

	request := httptest.NewRequest(http.MethodGet, "/weather", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, metricsRequest)
	body := metricsResponse.Body.String()
	if !strings.Contains(body, `weatherlookup_http_requests_total{method="GET",path="/weather",status="204"} 1`) {
		t.Fatalf("metrics missing request counter:\n%s", body)
	}
	if !strings.Contains(body, `weatherlookup_http_request_duration_seconds_bucket{method="GET",path="/weather",status="204",le="+Inf"} 1`) {
		t.Fatalf("metrics missing histogram:\n%s", body)
	}
	if strings.Contains(body, `path="/metrics"`) {
		t.Fatalf("metrics scrape should not be recorded:\n%s", body)
	}
}

func TestHTTPLabelsHaveBoundedCardinality(t *testing.T) {
	m := New()
	mux := http.NewServeMux()
	for _, path := range []string{"/weather", "/v1/weather", "/healthz"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	}
	handler := m.Middleware(mux)
	for i := 0; i < 5000; i++ {
		for _, method := range []string{"GET", fmt.Sprintf("CUSTOM%d", i)} {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, fmt.Sprintf("/missing/%d", i), nil))
		}
	}
	if len(m.requests) != 2 {
		t.Fatalf("unmatched paths/methods created %d series, want 2", len(m.requests))
	}
	for _, method := range []string{"GET", "OTHER"} {
		if s := m.requests[key{method: method, path: "unmatched", status: 404}]; s == nil || s.count != 5000 {
			t.Fatalf("incorrect unmatched series for %s: %+v", method, s)
		}
	}
	for _, path := range []string{"/weather", "/v1/weather", "/healthz"} {
		for _, method := range []string{"GET", "HEAD", "POST"} {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
			status := 200
			label := path
			if method == "POST" {
				status = 405
				label = "unmatched" // ServeMux does not match a pattern on 405.
			}
			if m.requests[key{method: method, path: label, status: status}] == nil {
				t.Fatalf("missing known route/method/status: %s %s %d", method, path, status)
			}
		}
	}
	if len(m.requests) != 9 {
		t.Fatalf("series = %d, want 9", len(m.requests))
	}
	scrape := httptest.NewRecorder()
	m.ServeHTTP(scrape, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(scrape.Body.String(), "/missing/") || strings.Contains(scrape.Body.String(), "CUSTOM") {
		t.Fatal("raw client labels leaked into metrics")
	}
}

func TestMatchedPatternsPreserveRoutesWithoutClientPathValues(t *testing.T) {
	m := New()
	mux := http.NewServeMux()
	for pattern, status := range map[string]int{
		"GET /weather/{location}":    200,
		"GET /objects/{rest...}":     404, // Application 404 still has a matched route.
		"GET /assets/":               204,
		"GET example.com/hosts/{id}": 200,
	} {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
	}
	handler := m.Middleware(mux)
	for i := 0; i < 1000; i++ {
		for _, path := range []string{
			fmt.Sprintf("/weather/city-%d?units=%d", i, i),
			fmt.Sprintf("/objects/dir-%d/file-%d", i, i),
			fmt.Sprintf("/assets/image-%d.png", i),
			fmt.Sprintf("/hosts/host-%d", i),
		} {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.com"+path, nil))
		}
	}
	if len(m.requests) != 4 {
		t.Fatalf("route series = %d, want 4", len(m.requests))
	}
	for path, status := range map[string]int{"/weather/{location}": 200, "/objects/{rest...}": 404, "/assets/": 204, "/hosts/{id}": 200} {
		if s := m.requests[key{method: "GET", path: path, status: status}]; s == nil || s.count != 1000 {
			t.Fatalf("incorrect matched pattern series for %s: %+v", path, s)
		}
	}
}

func TestMuxRedirectsHaveBoundedLabels(t *testing.T) {
	m := New()
	mux := http.NewServeMux()
	mux.HandleFunc("/folders/{id}/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	handler := m.Middleware(mux)
	for i := 0; i < 1000; i++ {
		for _, method := range []string{"GET", "CONNECT"} {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(method, fmt.Sprintf("/folders/folder-%d", i), nil)
			handler.ServeHTTP(w, r)
			if w.Code != 301 {
				t.Fatalf("%s: status = %d, want 301", method, w.Code)
			}
		}
	}
	if len(m.requests) != 2 {
		t.Fatalf("redirects created %d series, want 2", len(m.requests))
	}
	for method, path := range map[string]string{"GET": "/folders/{id}/", "CONNECT": "unmatched"} {
		if s := m.requests[key{method: method, path: path, status: 301}]; s == nil || s.count != 1000 {
			t.Fatalf("incorrect redirect series for %s: %+v", method, s)
		}
	}
}

func TestDomainMetricsAreExposed(t *testing.T) {
	metricSet := New()
	metricSet.CacheLookup("hit")
	metricSet.CacheStats(3, 2)
	metricSet.VendorRequest("forecast.current", "success", 20*time.Millisecond)
	metricSet.VendorRetry("forecast.current")
	metricSet.StaleResponse()
	metricSet.ResponseLogEnqueued(4)
	metricSet.ResponseLogWrite("success", 10*time.Millisecond)

	response := httptest.NewRecorder()
	metricSet.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, want := range []string{
		`weatherlookup_cache_requests_total{result="hit"} 1`,
		`weatherlookup_cache_entries 3`,
		`weatherlookup_cache_evictions_total 2`,
		`weatherlookup_vendor_requests_total{operation="forecast.current",outcome="success"} 1`,
		`weatherlookup_vendor_retries_total{operation="forecast.current"} 1`,
		`weatherlookup_stale_responses_total 1`,
		`weatherlookup_response_log_enqueued_total 1`,
		`weatherlookup_response_log_writes_total{outcome="success"} 1`,
		`weatherlookup_response_log_write_duration_seconds_bucket{le="+Inf"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
