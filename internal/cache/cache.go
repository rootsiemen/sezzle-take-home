// Package cache provides a bounded, thread-safe TTL cache.
package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry[T any] struct {
	key     string
	value   T
	created time.Time
}

// Stats describes the current cache size and cumulative evictions.
type Stats struct {
	Entries   int
	Evictions uint64
}

// Cache is an LRU cache that retains expired entries until they are replaced
// or evicted. Retaining expired values permits stale-if-error responses.
type Cache[T any] struct {
	mu        sync.Mutex
	ttl       time.Duration
	capacity  int
	now       func() time.Time
	entries   map[string]*list.Element
	order     *list.List
	evictions uint64
}

func New[T any](capacity int, ttl time.Duration) *Cache[T] {
	return NewWithClock[T](capacity, ttl, time.Now)
}

func NewWithClock[T any](capacity int, ttl time.Duration, now func() time.Time) *Cache[T] {
	if capacity < 0 {
		capacity = 0
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	if now == nil {
		now = time.Now
	}
	return &Cache[T]{
		ttl:      ttl,
		capacity: capacity,
		now:      now,
		entries:  make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get returns a fresh value. Expired values are deliberately left available
// through GetStale so callers can degrade gracefully when a vendor fails.
func (c *Cache[T]) Get(key string) (value T, age time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.entries[key]
	if !exists {
		return value, 0, false
	}
	item := element.Value.(*entry[T])
	age = c.age(item)
	if age >= c.ttl {
		return value, age, false
	}
	c.order.MoveToFront(element)
	return item.value, age, true
}

// GetStale returns an expired value retained in the cache. It does not return
// fresh values; callers should use Get for those.
func (c *Cache[T]) GetStale(key string) (value T, age time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.entries[key]
	if !exists {
		return value, 0, false
	}
	item := element.Value.(*entry[T])
	age = c.age(item)
	if age < c.ttl {
		return value, age, false
	}
	c.order.MoveToFront(element)
	return item.value, age, true
}

// Set stores a value and refreshes its TTL. Stale entries compete for the same
// bounded LRU capacity; unrelated inserts must not erase stale-if-error data.
func (c *Cache[T]) Set(key string, value T) Stats {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity == 0 {
		return c.stats()
	}
	now := c.now()
	if element, exists := c.entries[key]; exists {
		item := element.Value.(*entry[T])
		item.value = value
		item.created = now
		c.order.MoveToFront(element)
		return c.stats()
	}

	for len(c.entries) >= c.capacity {
		c.remove(c.order.Back())
	}
	element := c.order.PushFront(&entry[T]{key: key, value: value, created: now})
	c.entries[key] = element
	return c.stats()
}

func (c *Cache[T]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats()
}

func (c *Cache[T]) stats() Stats {
	return Stats{Entries: len(c.entries), Evictions: c.evictions}
}

func (c *Cache[T]) age(item *entry[T]) time.Duration {
	age := c.now().Sub(item.created)
	if age < 0 {
		return 0
	}
	return age
}

func (c *Cache[T]) remove(element *list.Element) {
	if element == nil {
		return
	}
	item := element.Value.(*entry[T])
	delete(c.entries, item.key)
	c.order.Remove(element)
	c.evictions++
}
