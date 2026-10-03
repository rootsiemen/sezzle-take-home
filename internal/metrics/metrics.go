// Package metrics provides the small Prometheus exposition surface used by
// the service. It intentionally has no third-party runtime dependency.
package metrics

import (
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var histogramBuckets = [...]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type key struct {
	method string
	path   string
	status int
}

type series struct {
	count        uint64
	durationSum  float64
	bucketCounts [len(histogramBuckets)]uint64
	infCount     uint64
}

type vendorKey struct {
	operation string
	outcome   string
}

// Metrics records HTTP request metrics and serves them in Prometheus text
// format. Labels are limited to method, route path, and status code.
type Metrics struct {
	mu                     sync.Mutex
	requests               map[key]*series
	inFlight               int64
	cacheLookups           map[string]uint64
	cacheEntries           int
	cacheEvictions         uint64
	staleResponses         uint64
	vendorRequests         map[vendorKey]*series
	vendorRetries          map[string]uint64
	responseLogEnqueued    uint64
	responseLogDropped     uint64
	responseLogDropReasons map[string]uint64
	responseLogWrites      map[string]uint64
	responseLogDurations   series
	responseLogRetries     uint64
	responseLogQueueDepth  int
	dbOpenConnections      int
	dbInUseConnections     int
	dbIdleConnections      int
	dbWaitCount            int64
	dbWaitDuration         time.Duration
	retentionDeleted       uint64
	guards                 guardMetrics
}

func New() *Metrics {
	return &Metrics{
		requests:               make(map[key]*series),
		cacheLookups:           make(map[string]uint64),
		vendorRequests:         make(map[vendorKey]*series),
		vendorRetries:          make(map[string]uint64),
		responseLogWrites:      make(map[string]uint64),
		responseLogDropReasons: make(map[string]uint64),
	}
}

func (m *Metrics) CacheLookup(result string) {
	m.mu.Lock()
	m.cacheLookups[result]++
	m.mu.Unlock()
}

func (m *Metrics) CacheStats(entries int, evictions uint64) {
	m.mu.Lock()
	m.cacheEntries = entries
	m.cacheEvictions = evictions
	m.mu.Unlock()
}

func (m *Metrics) VendorRequest(operation, outcome string, duration time.Duration) {
	m.mu.Lock()
	requestSeries := m.vendorRequests[vendorKey{operation: operation, outcome: outcome}]
	if requestSeries == nil {
		requestSeries = &series{}
		m.vendorRequests[vendorKey{operation: operation, outcome: outcome}] = requestSeries
	}
	observeDuration(requestSeries, duration.Seconds())
	m.mu.Unlock()
}

func (m *Metrics) VendorRetry(operation string) {
	m.mu.Lock()
	m.vendorRetries[operation]++
	m.mu.Unlock()
}

func (m *Metrics) StaleResponse() {
	m.mu.Lock()
	m.staleResponses++
	m.mu.Unlock()
}

func (m *Metrics) ResponseLogEnqueued(depth int) {
	m.mu.Lock()
	m.responseLogEnqueued++
	m.responseLogQueueDepth = depth
	m.mu.Unlock()
}

func (m *Metrics) ResponseLogDropped(depth int, reason string) {
	switch reason {
	case "queue_full", "schema_error", "write_failed":
	default:
		reason = "other"
	}
	m.mu.Lock()
	m.responseLogDropped++
	m.responseLogDropReasons[reason]++
	m.responseLogQueueDepth = depth
	m.mu.Unlock()
}

func (m *Metrics) ResponseLogQueueDepth(depth int) {
	m.mu.Lock()
	m.responseLogQueueDepth = depth
	m.mu.Unlock()
}

func (m *Metrics) ResponseLogWrite(outcome string, duration time.Duration) {
	m.mu.Lock()
	m.responseLogWrites[outcome]++
	observeDuration(&m.responseLogDurations, duration.Seconds())
	m.mu.Unlock()
}

func (m *Metrics) ResponseLogRetry() {
	m.mu.Lock()
	m.responseLogRetries++
	m.mu.Unlock()
}

func (m *Metrics) DatabasePool(stats sql.DBStats) {
	m.mu.Lock()
	m.dbOpenConnections = stats.OpenConnections
	m.dbInUseConnections = stats.InUse
	m.dbIdleConnections = stats.Idle
	m.dbWaitCount = stats.WaitCount
	m.dbWaitDuration = stats.WaitDuration
	m.mu.Unlock()
}

func (m *Metrics) RetentionDeleted(count int) {
	if count <= 0 {
		return
	}
	m.mu.Lock()
	m.retentionDeleted += uint64(count)
	m.mu.Unlock()
}

func observeDuration(requestSeries *series, duration float64) {
	requestSeries.count++
	requestSeries.durationSum += duration
	for index, bucket := range histogramBuckets {
		if duration <= bucket {
			requestSeries.bucketCounts[index]++
		}
	}
	requestSeries.infCount++
}

// Middleware records all routes except /metrics itself, preventing scrapes
// from changing the application traffic metrics they are inspecting.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/metrics" {
			next.ServeHTTP(response, request)
			return
		}

		started := time.Now()
		m.mu.Lock()
		m.inFlight++
		m.mu.Unlock()

		recorder := &responseRecorder{ResponseWriter: response, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		// ServeMux sets Pattern while dispatching. Read it only after the mux
		// runs; never fall back to URL.Path when no route matched (404/405).
		path := routeLabel(request.Pattern)
		// Go's CONNECT trailing-slash redirects can set Pattern to a concrete
		// request-derived redirect path, rather than a registered route pattern.
		if request.Method == http.MethodConnect && recorder.status == http.StatusMovedPermanently {
			path = "unmatched"
		}

		m.mu.Lock()
		m.inFlight--
		requestKey := key{method: methodLabel(request.Method), path: path, status: recorder.status}
		requestSeries := m.requests[requestKey]
		if requestSeries == nil {
			requestSeries = &series{}
			m.requests[requestKey] = requestSeries
		}
		requestSeries.count++
		duration := time.Since(started).Seconds()
		requestSeries.durationSum += duration
		for index, bucket := range histogramBuckets {
			if duration <= bucket {
				requestSeries.bucketCounts[index]++
			}
		}
		requestSeries.infCount++
		m.mu.Unlock()
	})
}

// Preserve the existing path label and static-route dashboard queries. Strip
// the optional method/host from the registered pattern, retaining wildcards
// such as /weather/{location} instead of the client's concrete path.
func routeLabel(pattern string) string {
	if index := strings.IndexByte(pattern, '/'); index >= 0 {
		return pattern[index:]
	}
	return "unmatched"
}

func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

func (m *Metrics) ServeHTTP(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response.WriteHeader(http.StatusOK)

	m.mu.Lock()
	keys := make([]key, 0, len(m.requests))
	for requestKey := range m.requests {
		keys = append(keys, requestKey)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].path != keys[j].path {
			return keys[i].path < keys[j].path
		}
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].status < keys[j].status
	})
	seriesByKey := make(map[key]series, len(keys))
	for _, requestKey := range keys {
		seriesByKey[requestKey] = *m.requests[requestKey]
	}
	inFlight := m.inFlight
	m.mu.Unlock()

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_http_requests_total Total HTTP requests handled by the service.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_http_requests_total counter")
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_http_request_duration_seconds HTTP request duration in seconds.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_http_request_duration_seconds histogram")
	for _, requestKey := range keys {
		requestSeries := seriesByKey[requestKey]
		labels := labelsFor(requestKey)
		_, _ = fmt.Fprintf(response, "weatherlookup_http_requests_total%s %d\n", labels, requestSeries.count)
		for index, bucket := range histogramBuckets {
			bucketLabels := labelsWithExtra(requestKey, "le", strconv.FormatFloat(bucket, 'g', -1, 64))
			_, _ = fmt.Fprintf(response, "weatherlookup_http_request_duration_seconds_bucket%s %d\n", bucketLabels, requestSeries.bucketCounts[index])
		}
		infLabels := labelsWithExtra(requestKey, "le", "+Inf")
		_, _ = fmt.Fprintf(response, "weatherlookup_http_request_duration_seconds_bucket%s %d\n", infLabels, requestSeries.infCount)
		_, _ = fmt.Fprintf(response, "weatherlookup_http_request_duration_seconds_sum%s %s\n", labels, strconv.FormatFloat(requestSeries.durationSum, 'g', -1, 64))
		_, _ = fmt.Fprintf(response, "weatherlookup_http_request_duration_seconds_count%s %d\n", labels, requestSeries.count)
	}

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_http_in_flight_requests Current number of HTTP requests being handled.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_http_in_flight_requests gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_http_in_flight_requests %d\n", inFlight)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_build_info Build information for the Weather Lookup service.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_build_info gauge")
	_, _ = fmt.Fprintln(response, `weatherlookup_build_info{version="1.0.0"} 1`)
	m.writeGuards(response)

	m.mu.Lock()
	cacheLookups := cloneCounters(m.cacheLookups)
	cacheEntries := m.cacheEntries
	cacheEvictions := m.cacheEvictions
	staleResponses := m.staleResponses
	vendorRequests := cloneSeriesMap(m.vendorRequests)
	vendorRetries := cloneCounters(m.vendorRetries)
	responseLogEnqueued := m.responseLogEnqueued
	responseLogDropped := m.responseLogDropped
	responseLogDropReasons := cloneCounters(m.responseLogDropReasons)
	responseLogWrites := cloneCounters(m.responseLogWrites)
	responseLogDurations := m.responseLogDurations
	responseLogRetries := m.responseLogRetries
	responseLogQueueDepth := m.responseLogQueueDepth
	dbOpenConnections := m.dbOpenConnections
	dbInUseConnections := m.dbInUseConnections
	dbIdleConnections := m.dbIdleConnections
	dbWaitCount := m.dbWaitCount
	dbWaitDuration := m.dbWaitDuration
	retentionDeleted := m.retentionDeleted
	m.mu.Unlock()

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_cache_requests_total Cache lookups by result.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_cache_requests_total counter")
	for result, count := range cacheLookups {
		_, _ = fmt.Fprintf(response, "weatherlookup_cache_requests_total{result=\"%s\"} %d\n", escape(result), count)
	}
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_cache_entries Current cache entries.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_cache_entries gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_cache_entries %d\n", cacheEntries)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_cache_evictions_total Cache entries evicted or expired.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_cache_evictions_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_cache_evictions_total %d\n", cacheEvictions)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_stale_responses_total Responses served from expired cache entries.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_stale_responses_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_stale_responses_total %d\n", staleResponses)

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_vendor_requests_total Vendor requests by operation and outcome.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_vendor_requests_total counter")
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_vendor_request_duration_seconds Vendor request duration.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_vendor_request_duration_seconds histogram")
	for requestKey, requestSeries := range vendorRequests {
		writeLabeledHistogram(response, "weatherlookup_vendor_request_duration_seconds", fmt.Sprintf(`{operation="%s",outcome="%s"}`, escape(requestKey.operation), escape(requestKey.outcome)), requestSeries)
		_, _ = fmt.Fprintf(response, "weatherlookup_vendor_requests_total{operation=\"%s\",outcome=\"%s\"} %d\n", escape(requestKey.operation), escape(requestKey.outcome), requestSeries.count)
	}

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_vendor_retries_total Vendor retry attempts.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_vendor_retries_total counter")
	for operation, count := range vendorRetries {
		_, _ = fmt.Fprintf(response, "weatherlookup_vendor_retries_total{operation=\"%s\"} %d\n", escape(operation), count)
	}

	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_enqueued_total Response records queued for persistence.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_enqueued_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_response_log_enqueued_total %d\n", responseLogEnqueued)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_dropped_total Response records lost to queue overflow or database failure.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_dropped_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_response_log_dropped_total %d\n", responseLogDropped)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_dropped_by_reason_total Lost response records by bounded reason.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_dropped_by_reason_total counter")
	for reason, count := range responseLogDropReasons {
		_, _ = fmt.Fprintf(response, "weatherlookup_response_log_dropped_by_reason_total{reason=\"%s\"} %d\n", reason, count)
	}
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_queue_depth Current response persistence queue depth.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_queue_depth gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_response_log_queue_depth %d\n", responseLogQueueDepth)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_writes_total Response persistence attempts by outcome.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_writes_total counter")
	for outcome, count := range responseLogWrites {
		_, _ = fmt.Fprintf(response, "weatherlookup_response_log_writes_total{outcome=\"%s\"} %d\n", escape(outcome), count)
	}
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_retries_total Response persistence retry attempts.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_retries_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_response_log_retries_total %d\n", responseLogRetries)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_response_log_write_duration_seconds Response persistence duration.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_response_log_write_duration_seconds histogram")
	writeLabeledHistogram(response, "weatherlookup_response_log_write_duration_seconds", "", responseLogDurations)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_retention_deleted_total Response records removed by retention cleanup.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_retention_deleted_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_retention_deleted_total %d\n", retentionDeleted)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_database_open_connections Current open database connections.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_database_open_connections gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_database_open_connections %d\n", dbOpenConnections)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_database_in_use_connections Current in-use database connections.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_database_in_use_connections gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_database_in_use_connections %d\n", dbInUseConnections)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_database_idle_connections Current idle database connections.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_database_idle_connections gauge")
	_, _ = fmt.Fprintf(response, "weatherlookup_database_idle_connections %d\n", dbIdleConnections)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_database_wait_count_total Database connection wait count.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_database_wait_count_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_database_wait_count_total %d\n", dbWaitCount)
	_, _ = fmt.Fprintln(response, "# HELP weatherlookup_database_wait_duration_seconds_total Database connection wait duration.")
	_, _ = fmt.Fprintln(response, "# TYPE weatherlookup_database_wait_duration_seconds_total counter")
	_, _ = fmt.Fprintf(response, "weatherlookup_database_wait_duration_seconds_total %s\n", strconv.FormatFloat(dbWaitDuration.Seconds(), 'g', -1, 64))
}

func cloneCounters(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneSeriesMap(source map[vendorKey]*series) map[vendorKey]series {
	clone := make(map[vendorKey]series, len(source))
	for key, value := range source {
		clone[key] = *value
	}
	return clone
}

func writeLabeledHistogram(response http.ResponseWriter, name, labels string, requestSeries series) {
	for index, bucket := range histogramBuckets {
		_, _ = fmt.Fprintf(response, "%s_bucket%s %d\n", name, histogramLabels(labels, strconv.FormatFloat(bucket, 'g', -1, 64)), requestSeries.bucketCounts[index])
	}
	_, _ = fmt.Fprintf(response, "%s_bucket%s %d\n", name, histogramLabels(labels, "+Inf"), requestSeries.infCount)
	_, _ = fmt.Fprintf(response, "%s_sum%s %s\n", name, labels, strconv.FormatFloat(requestSeries.durationSum, 'g', -1, 64))
	_, _ = fmt.Fprintf(response, "%s_count%s %d\n", name, labels, requestSeries.count)
}

func histogramLabels(labels, valueText string) string {
	if labels == "" {
		return fmt.Sprintf(`{le="%s"}`, valueText)
	}
	return strings.TrimSuffix(labels, "}") + fmt.Sprintf(`,le="%s"}`, valueText)
}

func labelsFor(requestKey key) string {
	return fmt.Sprintf(`{method="%s",path="%s",status="%d"}`,
		escape(requestKey.method), escape(requestKey.path), requestKey.status)
}

func labelsWithExtra(requestKey key, name, value string) string {
	labels := labelsFor(requestKey)
	return "{" + strings.TrimSuffix(strings.TrimPrefix(labels, "{"), "}") + "," + name + `="` + escape(value) + `"}`
}

func escape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(body)
}
