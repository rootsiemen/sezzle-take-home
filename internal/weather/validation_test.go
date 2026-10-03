package weather

import (
	"context"
	"strings"
	"testing"
	"time"

	"weatherlookup/internal/cache"
)

func TestServiceValidatesLocationBeforeCacheAndVendor(t *testing.T) {
	for _, name := range []string{strings.Repeat("x", 257), strings.Repeat("é", 129), strings.Repeat(" ", 256) + "Paris", "Paris\x00", "Paris\xff"} {
		provider := &fakeProvider{}
		cached := cache.New[Result](10, time.Minute)
		query := Query{Location: name}
		// Even a preexisting cache entry must not bypass validation.
		cached.Set(cacheKey(query), Result{})
		s := NewServiceWithOptions(provider, Options{Cache: cached})
		_, metadata, err := s.LookupWithMetadata(context.Background(), query)
		if err == nil || metadata.CacheStatus != CacheDisabled || provider.searches != 0 || provider.lookups != 0 {
			t.Fatalf("invalid location reached cache/vendor: metadata=%+v, calls=%d/%d, err=%v", metadata, provider.searches, provider.lookups, err)
		}
	}
}

func TestServiceAcceptsLocationByteBoundary(t *testing.T) {
	provider := &fakeProvider{}
	s := NewService(provider)
	if _, err := s.Lookup(context.Background(), Query{Location: strings.Repeat("é", 128)}); err != nil {
		t.Fatal(err)
	}
	if provider.searches != 1 || provider.lookups != 1 {
		t.Fatalf("provider calls=%d/%d", provider.searches, provider.lookups)
	}
}
