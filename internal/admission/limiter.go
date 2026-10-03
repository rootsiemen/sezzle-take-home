// Package admission implements fail-fast concurrency limits with no wait queue.
package admission

import "sync"

type Limiter struct {
	mu              sync.Mutex
	active, maximum int
	// Called under the lock to keep metric snapshots ordered. Must not reenter.
	onChange func(active, maximum int)
}

func New(maximum int, onChange func(int, int)) *Limiter {
	if maximum <= 0 {
		panic("admission limit must be positive")
	}
	l := &Limiter{maximum: maximum, onChange: onChange}
	l.observe()
	return l
}

func (l *Limiter) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active >= l.maximum {
		return false
	}
	l.active++
	l.observe()
	return true
}

// Release must be called exactly once for each successful TryAcquire.
func (l *Limiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active == 0 {
		panic("admission permit released without acquisition")
	}
	l.active--
	l.observe()
}

func (l *Limiter) observe() {
	if l.onChange != nil {
		l.onChange(l.active, l.maximum)
	}
}
