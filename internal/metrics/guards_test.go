package metrics

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGuardMetricsAreBoundedAndDescribeAdmissionAndCircuits(t *testing.T) {
	m := New()
	m.LookupAdmission(2, 64)
	m.LookupRejected()
	m.VendorAdmission(1, 8)
	m.VendorRejected()
	m.CircuitState("forecast.current", 1)
	m.CircuitRejected("forecast.current")
	for i := 0; i < 1000; i++ {
		m.CircuitState(fmt.Sprintf("custom-%d", i), 1)
		m.CircuitRejected(fmt.Sprintf("custom-%d", i))
	}
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, sample := range []string{
		"weatherlookup_lookup_in_flight 2", "weatherlookup_lookup_max_in_flight 64", "weatherlookup_lookup_rejected_total 1",
		"weatherlookup_vendor_in_flight 1", "weatherlookup_vendor_max_in_flight 8", "weatherlookup_vendor_admission_rejected_total 1",
		`weatherlookup_circuit_state{operation="geocoding.search"} 0`, `weatherlookup_circuit_state{operation="forecast.current"} 1`,
		`weatherlookup_circuit_rejections_total{operation="forecast.current"} 1`,
	} {
		if !strings.Contains(w.Body.String(), sample+"\n") {
			t.Fatalf("missing %s", sample)
		}
	}
	if strings.Contains(w.Body.String(), "custom-") {
		t.Fatal("circuit label cardinality is unbounded")
	}
}
