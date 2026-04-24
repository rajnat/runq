package observability

import (
	"strings"
	"testing"
)

func TestRegistryRenderIncludesRecordedMetrics(t *testing.T) {
	r := NewRegistry()
	r.IncCounter("runq_test_counter_total")
	r.SetGauge("runq_test_gauge", 7)
	r.ObserveHistogram("runq_test_histogram_seconds", 0.25)
	r.IncCounterVec("runq_test_counter_vec_total", map[string]string{"route": "/v1/jobs", "status": "2xx"})

	output := r.Render()
	for _, want := range []string{
		"runq_test_counter_total",
		"runq_test_gauge 7",
		"runq_test_histogram_seconds_bucket",
		"runq_test_counter_vec_total{route=\"/v1/jobs\",status=\"2xx\"} 1",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", want, output)
		}
	}
}
