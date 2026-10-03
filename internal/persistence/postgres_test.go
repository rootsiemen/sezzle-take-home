package persistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"weatherlookup/internal/response"
)

func TestLoggerPersistsQueuedResponse(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	occurredAt := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	mock.ExpectExec("(?s)CREATE TABLE IF NOT EXISTS weather_responses").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("(?s)INSERT INTO weather_responses").
		WithArgs(occurredAt, "GET", "/weather", "coordinates", nil, 1.0, 2.0, 200, int64(14), "miss", int64(0), []byte(`{"ok":true}`), nil, "trace-id", "span-id").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	logger, err := New(Config{DB: db, QueueSize: 1, Retention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	logger.Enqueue(response.Record{
		OccurredAt:  occurredAt,
		Method:      "GET",
		Path:        "/weather",
		QueryMode:   "coordinates",
		Latitude:    floatPointer(1),
		Longitude:   floatPointer(2),
		Status:      200,
		DurationMS:  14,
		CacheStatus: "miss",
		Response:    json.RawMessage(`{"ok":true}`),
		TraceID:     "trace-id",
		SpanID:      "span-id",
	})

	closeContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := logger.Close(closeContext); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDSNFromConfigEscapesCredentials(t *testing.T) {
	dsn := DSNFromConfig("postgres.example", "5432", "weather", "weather user", "p@ss word")
	if dsn != "postgres://weather%20user:p%40ss%20word@postgres.example:5432/weather?sslmode=disable" {
		t.Fatalf("dsn = %q", dsn)
	}
}

func floatPointer(value float64) *float64 {
	return &value
}
