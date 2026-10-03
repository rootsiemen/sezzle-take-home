package validation

import (
	"strings"
	"testing"
)

func TestLocation(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
		invalid           bool
	}{
		{name: "absent"},
		{name: "trimmed Unicode", input: " São Paulo ", want: "São Paulo"},
		{name: "ASCII boundary", input: strings.Repeat("a", 256), want: strings.Repeat("a", 256)},
		{name: "Unicode boundary", input: strings.Repeat("é", 128), want: strings.Repeat("é", 128)},
		{name: "ASCII too long", input: strings.Repeat("a", 257), invalid: true},
		{name: "Unicode too long", input: strings.Repeat("é", 129), invalid: true},
		{name: "whitespace counts", input: strings.Repeat(" ", 256) + "Paris", invalid: true},
		{name: "invalid UTF-8", input: "Paris\xff", invalid: true},
		{name: "NUL", input: "Paris\x00", invalid: true},
		{name: "embedded control", input: "New\nYork", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Location(tc.input)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("Location() = %q, %v; want %q, invalid=%v", got, err, tc.want, tc.invalid)
			}
			if err != nil && strings.Contains(err.Error(), tc.input) {
				t.Fatal("validation error includes invalid input")
			}
		})
	}
}
