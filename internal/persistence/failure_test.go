package persistence

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XSAM/otelsql"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"weatherlookup/internal/metrics"
	"weatherlookup/internal/response"
)

func assertMetrics(t *testing.T, m *metrics.Metrics, samples ...string) {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, sample := range samples {
		if !strings.Contains(w.Body.String(), sample+"\n") {
			t.Fatalf("missing %s in metrics:\n%s", sample, w.Body.String())
		}
	}
}

func TestDatabaseFailuresCountOneLostRecord(t *testing.T) {
	for _, reason := range []string{"schema_error", "write_failed", "retry_succeeds"} {
		t.Run(reason, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			m := metrics.New()
			l := &Logger{db: db, queue: make(chan response.Record, 1), metrics: m,
				logger: slog.New(slog.NewTextHandler(io.Discard, nil)), tracer: noop.NewTracerProvider().Tracer("test")}
			failure := errors.New("database unavailable")
			schema := mock.ExpectExec("(?s)CREATE TABLE IF NOT EXISTS weather_responses")
			if reason == "schema_error" {
				schema.WillReturnError(failure)
			} else {
				schema.WillReturnResult(sqlmock.NewResult(0, 0))
				for i := 0; i < 3; i++ {
					insert := mock.ExpectExec("(?s)INSERT INTO weather_responses")
					if reason == "retry_succeeds" && i == 2 {
						insert.WillReturnResult(sqlmock.NewResult(1, 1))
					} else {
						insert.WillReturnError(failure)
					}
				}
			}
			l.persist(response.Record{OccurredAt: time.Now(), Method: "GET", Path: "/weather"})
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if reason == "retry_succeeds" {
				assertMetrics(t, m, "weatherlookup_response_log_dropped_total 0", "weatherlookup_response_log_retries_total 2",
					`weatherlookup_response_log_writes_total{outcome="success"} 1`)
			} else {
				assertMetrics(t, m, "weatherlookup_response_log_dropped_total 1", `weatherlookup_response_log_dropped_by_reason_total{reason="`+reason+`"} 1`)
			}
			if reason == "write_failed" {
				assertMetrics(t, m, `weatherlookup_response_log_writes_total{outcome="error"} 3`, "weatherlookup_response_log_retries_total 2")
			}
			// An outage must not poison schema initialization or later records.
			if reason == "schema_error" {
				mock.ExpectExec("(?s)CREATE TABLE IF NOT EXISTS weather_responses").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectExec("(?s)INSERT INTO weather_responses").WillReturnResult(sqlmock.NewResult(2, 1))
			l.persist(response.Record{OccurredAt: time.Now(), Method: "GET", Path: "/weather"})
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if reason != "retry_succeeds" {
				assertMetrics(t, m, "weatherlookup_response_log_dropped_total 1", `weatherlookup_response_log_writes_total{outcome="success"} 1`)
			}
			mock.ExpectClose()
		})
	}
}

func TestQueueOverflowCountsOnlyRejectedRecords(t *testing.T) {
	m := metrics.New()
	l := &Logger{queue: make(chan response.Record, 1), metrics: m, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	l.Enqueue(response.Record{})
	l.Enqueue(response.Record{})
	assertMetrics(t, m, "weatherlookup_response_log_enqueued_total 1", "weatherlookup_response_log_dropped_total 1",
		`weatherlookup_response_log_dropped_by_reason_total{reason="queue_full"} 1`, "weatherlookup_response_log_queue_depth 1")
}

func TestAsyncSQLSpansBelongToOriginatingRequest(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	dsn := t.Name()
	rawDB, mock, err := sqlmock.NewWithDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()
	db, err := otelsql.Open("sqlmock", dsn, otelsql.WithTracerProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("(?s)CREATE TABLE IF NOT EXISTS weather_responses").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("(?s)INSERT INTO weather_responses").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose() // instrumented pool
	mock.ExpectClose() // original mock pool
	l, err := New(Config{DB: db, TracerProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	requestContext, cancel := context.WithCancel(context.Background())
	_, requestSpan := provider.Tracer("test").Start(requestContext, "http.request")
	sc := requestSpan.SpanContext()
	requestSpan.End()
	cancel() // persistence is deliberately queued after the request has ended
	l.Enqueue(response.Record{OccurredAt: time.Now(), Method: "GET", Path: "/weather",
		TraceID: sc.TraceID().String(), SpanID: sc.SpanID().String(), SpanContext: sc})
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := l.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var persistID trace.SpanID
	for _, span := range recorder.Ended() {
		if span.Name() == "response.persist" {
			if span.Parent().SpanID() != sc.SpanID() || span.SpanContext().TraceID() != sc.TraceID() {
				t.Fatal("persistence span lost request parent")
			}
			persistID = span.SpanContext().SpanID()
		}
	}
	if !persistID.IsValid() {
		t.Fatal("missing persistence span")
	}
	sqlSpans := 0
	for _, span := range recorder.Ended() {
		if span.Name() == "sql.conn.exec" {
			sqlSpans++
			if span.Parent().SpanID() != persistID || span.SpanContext().TraceID() != sc.TraceID() {
				t.Fatalf("SQL span disconnected: %s", span.Name())
			}
		}
	}
	if sqlSpans != 2 {
		t.Fatalf("SQL spans = %d, want schema and insert spans", sqlSpans)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
