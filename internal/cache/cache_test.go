package cache

import (
	"testing"
	"time"
)

func TestCacheFreshAndStaleValues(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	cache := NewWithClock[string](2, 5*time.Minute, func() time.Time { return now })
	cache.Set("weather", "sunny")

	value, age, ok := cache.Get("weather")
	if !ok || value != "sunny" || age != 0 {
		t.Fatalf("fresh lookup = %q, %s, %v", value, age, ok)
	}

	now = now.Add(6 * time.Minute)
	if _, _, ok := cache.Get("weather"); ok {
		t.Fatal("expired value returned as fresh")
	}
	value, age, ok = cache.GetStale("weather")
	if !ok || value != "sunny" || age != 6*time.Minute {
		t.Fatalf("stale lookup = %q, %s, %v", value, age, ok)
	}
}

func TestExpiryBoundaryAndStaleRetentionOnInsert(t *testing.T) {
	now := time.Now()
	c := NewWithClock[string](2, time.Minute, func() time.Time { return now })
	c.Set("a", "sunny")
	now = now.Add(time.Minute)
	if _, _, ok := c.Get("a"); ok {
		t.Fatal("entry is fresh at exact TTL")
	}
	c.Set("b", "rainy")
	if value, age, ok := c.GetStale("a"); !ok || value != "sunny" || age != time.Minute {
		t.Fatalf("unrelated insert removed stale fallback: %q %s %v", value, age, ok)
	}
	c.Set("c", "cloudy")
	if stats := c.Stats(); stats.Entries != 2 || stats.Evictions != 1 {
		t.Fatalf("stale retention violated capacity: %+v", stats)
	}
}

func TestCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	now := time.Now()
	cache := NewWithClock[string](2, time.Hour, func() time.Time { return now })
	cache.Set("a", "one")
	cache.Set("b", "two")
	if _, _, ok := cache.Get("a"); !ok {
		t.Fatal("expected a to be present")
	}
	cache.Set("c", "three")
	if _, _, ok := cache.Get("b"); ok {
		t.Fatal("least recently used entry was not evicted")
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Evictions != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}
