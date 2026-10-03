package metrics

import (
	"fmt"
	"net/http"
)

type guardMetrics struct {
	lookupActive, lookupLimit      int
	vendorActive, vendorLimit      int
	lookupRejected, vendorRejected uint64
	circuitStates                  [2]int
	circuitRejected                [2]uint64
}

var circuitOperations = [...]string{"geocoding.search", "forecast.current"}

func (m *Metrics) LookupAdmission(active, maximum int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guards.lookupActive, m.guards.lookupLimit = active, maximum
}

func (m *Metrics) LookupRejected() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guards.lookupRejected++
}

func (m *Metrics) VendorAdmission(active, maximum int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guards.vendorActive, m.guards.vendorLimit = active, maximum
}

func (m *Metrics) VendorRejected() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guards.vendorRejected++
}

func (m *Metrics) CircuitState(operation string, state int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for index, name := range circuitOperations {
		if operation == name && state >= 0 && state <= 2 {
			m.guards.circuitStates[index] = state
		}
	}
}

func (m *Metrics) CircuitRejected(operation string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for index, name := range circuitOperations {
		if operation == name {
			m.guards.circuitRejected[index]++
		}
	}
}

func (m *Metrics) writeGuards(w http.ResponseWriter) {
	m.mu.Lock()
	g := m.guards
	m.mu.Unlock()
	for _, sample := range []struct {
		name, help, kind string
		value            int
	}{
		{"weatherlookup_lookup_in_flight", "Active admitted weather HTTP requests.", "gauge", g.lookupActive},
		{"weatherlookup_lookup_max_in_flight", "Maximum admitted weather HTTP requests per process.", "gauge", g.lookupLimit},
		{"weatherlookup_vendor_in_flight", "Active vendor-backed lookups including retries and detached fills.", "gauge", g.vendorActive},
		{"weatherlookup_vendor_max_in_flight", "Maximum vendor-backed lookups per process.", "gauge", g.vendorLimit},
	} {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", sample.name, sample.help, sample.name, sample.kind, sample.name, sample.value)
	}
	_, _ = fmt.Fprintln(w, "# HELP weatherlookup_lookup_rejected_total Weather HTTP requests rejected by admission control.")
	_, _ = fmt.Fprintln(w, "# TYPE weatherlookup_lookup_rejected_total counter")
	_, _ = fmt.Fprintf(w, "weatherlookup_lookup_rejected_total %d\n", g.lookupRejected)
	_, _ = fmt.Fprintln(w, "# HELP weatherlookup_vendor_admission_rejected_total Vendor-backed lookups rejected because capacity was exhausted.")
	_, _ = fmt.Fprintln(w, "# TYPE weatherlookup_vendor_admission_rejected_total counter")
	_, _ = fmt.Fprintf(w, "weatherlookup_vendor_admission_rejected_total %d\n", g.vendorRejected)
	_, _ = fmt.Fprintln(w, "# HELP weatherlookup_circuit_state Vendor circuit state: 0 closed, 1 open, 2 half-open (one recovery probe).")
	_, _ = fmt.Fprintln(w, "# TYPE weatherlookup_circuit_state gauge")
	_, _ = fmt.Fprintln(w, "# HELP weatherlookup_circuit_rejections_total Vendor attempts rejected by an open or probing circuit.")
	_, _ = fmt.Fprintln(w, "# TYPE weatherlookup_circuit_rejections_total counter")
	for index, operation := range circuitOperations {
		_, _ = fmt.Fprintf(w, "weatherlookup_circuit_state{operation=\"%s\"} %d\n", operation, g.circuitStates[index])
		_, _ = fmt.Fprintf(w, "weatherlookup_circuit_rejections_total{operation=\"%s\"} %d\n", operation, g.circuitRejected[index])
	}
}
