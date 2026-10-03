// Package persistence asynchronously stores sanitized API responses in
// PostgreSQL without placing the database on the request critical path.
package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"weatherlookup/internal/metrics"
	"weatherlookup/internal/response"
	"weatherlookup/internal/validation"
)

const createTableStatement = `
CREATE TABLE IF NOT EXISTS weather_responses (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    query_mode TEXT NOT NULL DEFAULT '',
    location TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    status INTEGER NOT NULL,
    duration_ms BIGINT NOT NULL,
    cache_status TEXT NOT NULL DEFAULT '',
    cache_age_seconds BIGINT NOT NULL DEFAULT 0,
    response_json JSONB,
    error_message TEXT,
    trace_id TEXT,
    span_id TEXT
);
CREATE INDEX IF NOT EXISTS weather_responses_occurred_at_idx ON weather_responses (occurred_at);
CREATE INDEX IF NOT EXISTS weather_responses_path_status_idx ON weather_responses (path, status);
CREATE INDEX IF NOT EXISTS weather_responses_cache_status_idx ON weather_responses (cache_status);
`

type Config struct {
	DSN            string
	QueueSize      int
	Workers        int
	Retention      time.Duration
	Logger         *slog.Logger
	Metrics        *metrics.Metrics
	Now            func() time.Time
	DB             *sql.DB
	TracerProvider trace.TracerProvider
}

// DSNFromConfig builds a URL-form PostgreSQL DSN while safely escaping user
// names and passwords. DATABASE_URL can still be used for externally managed
// databases.
func DSNFromConfig(host, port, database, username, password string) string {
	if strings.TrimSpace(host) == "" {
		return ""
	}
	if port == "" {
		port = "5432"
	}
	if database == "" {
		database = "weatherlookup"
	}
	dsn := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	dsn.User = url.UserPassword(username, password)
	return dsn.String()
}

type Logger struct {
	db        *sql.DB
	queue     chan response.Record
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	logger    *slog.Logger
	metrics   *metrics.Metrics
	now       func() time.Time
	retention time.Duration
	tracer    trace.Tracer

	schemaMu    sync.Mutex
	schemaReady bool
}

// New creates an asynchronous response logger. An empty DSN disables
// persistence and returns a nil logger so standalone runs remain lightweight.
func New(config Config) (*Logger, error) {
	if config.DB == nil && strings.TrimSpace(config.DSN) == "" {
		return nil, nil
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.Retention <= 0 {
		config.Retention = 7 * 24 * time.Hour
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.TracerProvider == nil {
		config.TracerProvider = otel.GetTracerProvider()
	}

	db := config.DB
	var err error
	if db == nil {
		db, err = otelsql.Open("pgx", config.DSN, otelsql.WithTracerProvider(config.TracerProvider))
		if err != nil {
			return nil, err
		}
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	logger := &Logger{
		db:        db,
		queue:     make(chan response.Record, config.QueueSize),
		stop:      make(chan struct{}),
		logger:    config.Logger,
		metrics:   config.Metrics,
		now:       config.Now,
		retention: config.Retention,
		tracer:    config.TracerProvider.Tracer("weatherlookup/persistence"),
	}
	for worker := 0; worker < config.Workers; worker++ {
		logger.wg.Add(1)
		go logger.worker()
	}
	logger.wg.Add(1)
	go logger.maintenance()
	return logger, nil
}

// Enqueue never blocks the HTTP handler. The newest record is dropped when
// the bounded queue is full.
func (l *Logger) Enqueue(record response.Record) {
	if l == nil {
		return
	}
	// Defend the queue/database boundary even for non-HTTP record producers.
	location, err := validation.Location(record.Location)
	if err != nil {
		record.Location = ""
		if record.Error == "" {
			record.Error = "invalid location omitted from response record"
		}
	} else {
		record.Location = location
	}
	if len(record.Response) > 64*1024 {
		record.Response = nil
		record.Error = "response body omitted because it exceeded the persistence limit"
	}
	select {
	case l.queue <- record:
		l.observeQueue()
		if l.metrics != nil {
			l.metrics.ResponseLogEnqueued(len(l.queue))
		}
	default:
		l.drop("queue_full")
		l.logger.Warn("response log dropped because persistence queue is full", "queue_depth", len(l.queue))
	}
}

func (l *Logger) worker() {
	defer l.wg.Done()
	for {
		select {
		case record := <-l.queue:
			l.persist(record)
		case <-l.stop:
			for {
				select {
				case record := <-l.queue:
					l.persist(record)
				default:
					return
				}
			}
		}
	}
}

func (l *Logger) persist(record response.Record) {
	// Async work outlives the HTTP request, but remains in its trace. Carry
	// only SpanContext, not the request context/deadline or arbitrary values.
	parent := trace.ContextWithSpanContext(context.Background(), record.SpanContext)
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	ctx, span := l.tracer.Start(ctx, "response.persist")
	defer span.End()
	if err := l.ensureSchema(ctx); err != nil {
		l.observeWrite(outcome(err), 0)
		l.drop("schema_error")
		span.RecordError(err)
		span.SetStatus(codes.Error, "response log schema unavailable")
		l.logger.Warn("response log schema unavailable", "error", err)
		l.observeQueue()
		return
	}

	var responseJSON any
	if len(record.Response) > 0 && json.Valid(record.Response) {
		responseJSON = []byte(record.Response)
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		started := time.Now()
		_, err := l.db.ExecContext(ctx, `
INSERT INTO weather_responses (
    occurred_at, method, path, query_mode, location, latitude, longitude,
    status, duration_ms, cache_status, cache_age_seconds, response_json,
    error_message, trace_id, span_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
			record.OccurredAt, record.Method, record.Path, record.QueryMode, nullableString(record.Location),
			record.Latitude, record.Longitude, record.Status, record.DurationMS, record.CacheStatus,
			record.CacheAgeSeconds, responseJSON, nullableString(record.Error), nullableString(record.TraceID),
			nullableString(record.SpanID))
		l.observeWrite(outcome(err), time.Since(started))
		if err == nil {
			l.observeQueue()
			return
		}
		lastErr = err
		if attempt < 3 {
			if l.metrics != nil {
				l.metrics.ResponseLogRetry()
			}
			if wait(ctx, time.Duration(attempt)*100*time.Millisecond) != nil {
				break
			}
		}
	}
	l.drop("write_failed")
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "response log insert failed")
	l.logger.Warn("response log insert failed", "error", lastErr)
	l.observeQueue()
}

func (l *Logger) drop(reason string) {
	if l.metrics != nil {
		l.metrics.ResponseLogDropped(len(l.queue), reason)
	}
}

func (l *Logger) ensureSchema(ctx context.Context) error {
	l.schemaMu.Lock()
	defer l.schemaMu.Unlock()
	if l.schemaReady {
		return nil
	}
	if _, err := l.db.ExecContext(ctx, createTableStatement); err != nil {
		return err
	}
	l.schemaReady = true
	return nil
}

func (l *Logger) maintenance() {
	defer l.wg.Done()
	initial := time.NewTimer(time.Second)
	defer initial.Stop()
	select {
	case <-initial.C:
		l.runMaintenance()
	case <-l.stop:
		return
	}

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.runMaintenance()
		case <-l.stop:
			return
		}
	}
}

func (l *Logger) runMaintenance() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.ensureSchema(ctx); err != nil {
		l.logger.Warn("response log schema migration failed", "error", err)
		return
	}
	cutoff := l.now().Add(-l.retention)
	result, err := l.db.ExecContext(ctx, "DELETE FROM weather_responses WHERE occurred_at < $1", cutoff)
	if err != nil {
		l.logger.Warn("response log retention cleanup failed", "error", err)
		return
	}
	deleted, err := result.RowsAffected()
	if err == nil && deleted > 0 && l.metrics != nil {
		l.metrics.RetentionDeleted(int(deleted))
	}
}

func (l *Logger) observeWrite(outcome string, duration time.Duration) {
	if l.metrics != nil {
		l.metrics.ResponseLogWrite(outcome, duration)
		l.metrics.DatabasePool(l.db.Stats())
	}
}

func (l *Logger) observeQueue() {
	if l.metrics != nil {
		l.metrics.ResponseLogQueueDepth(len(l.queue))
	}
}

func (l *Logger) Close(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() { close(l.stop) })
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return l.db.Close()
	case <-ctx.Done():
		_ = l.db.Close()
		return ctx.Err()
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func outcome(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "error"
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
