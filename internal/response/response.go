// Package response contains the response record shared by HTTP and
// persistence layers.
package response

import (
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Record is the sanitized, structured representation of one application
// response. Response contains JSON only and is bounded by the HTTP layer.
type Record struct {
	OccurredAt      time.Time
	Method          string
	Path            string
	QueryMode       string
	Location        string
	Latitude        *float64
	Longitude       *float64
	Status          int
	DurationMS      int64
	CacheStatus     string
	CacheAgeSeconds int64
	Response        json.RawMessage
	Error           string
	TraceID         string
	SpanID          string
	// SpanContext preserves sampling flags and trace state without retaining
	// the HTTP request or its cancellation. It is not a database column.
	SpanContext trace.SpanContext
}

// Sink accepts records without coupling request latency to persistence.
type Sink interface {
	Enqueue(Record)
}
