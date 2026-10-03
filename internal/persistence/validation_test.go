package persistence

import (
	"strings"
	"testing"

	"weatherlookup/internal/response"
)

func TestEnqueueValidatesLocation(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
		invalid           bool
	}{
		{name: "too long", input: strings.Repeat("x", 257), invalid: true},
		{name: "Unicode too long", input: strings.Repeat("é", 129), invalid: true},
		{name: "invalid UTF-8", input: "Paris\xff", invalid: true},
		{name: "NUL", input: "Paris\x00", invalid: true},
		{name: "boundary", input: strings.Repeat("é", 128), want: strings.Repeat("é", 128)},
		{name: "trimmed", input: " São Paulo ", want: "São Paulo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &Logger{queue: make(chan response.Record, 1)}
			l.Enqueue(response.Record{Location: tc.input})
			record := <-l.queue
			if record.Location != tc.want || (record.Error != "") != tc.invalid {
				t.Fatalf("record = %+v", record)
			}
			l.Enqueue(response.Record{Location: tc.input, Error: "original rejection"})
			if record := <-l.queue; record.Error != "original rejection" {
				t.Fatalf("original rejection overwritten: %+v", record)
			}
		})
	}
}
