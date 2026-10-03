package admission

import (
	"sync"
	"testing"
)

func TestLimiterBoundsConcurrentAdmissionsAndReusesPermits(t *testing.T) {
	active, peak := 0, 0
	l := New(4, func(current, maximum int) {
		active = current
		if current > peak {
			peak = current
		}
		if current < 0 || current > maximum {
			t.Errorf("invalid occupancy %d/%d", current, maximum)
		}
	})
	var wg sync.WaitGroup
	results := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- l.TryAcquire() }()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for ok := range results {
		if ok {
			accepted++
		}
	}
	if accepted != 4 || active != 4 || peak != 4 {
		t.Fatalf("accepted/active/peak = %d/%d/%d", accepted, active, peak)
	}
	for i := 0; i < accepted; i++ {
		l.Release()
	}
	if active != 0 || !l.TryAcquire() {
		t.Fatal("released capacity was not reusable")
	}
	l.Release()
}
