// Package validation defines input rules shared by the API, service, and sinks.
package validation

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxLocationBytes applies to the decoded input, including surrounding space.
const MaxLocationBytes = 256

// Location validates before trimming so oversized whitespace is also rejected.
// Empty input is allowed here: the caller decides whether coordinates or a
// nonempty place name are required. Errors never include the supplied input.
func Location(value string) (string, error) {
	if len(value) > MaxLocationBytes {
		return "", fmt.Errorf("location must be at most %d bytes", MaxLocationBytes)
	}
	if !utf8.ValidString(value) {
		return "", errors.New("location must be valid UTF-8")
	}
	value = strings.TrimSpace(value)
	if strings.ContainsFunc(value, unicode.IsControl) {
		return "", errors.New("location must not contain control characters")
	}
	return value, nil
}
