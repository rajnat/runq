package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
)

type Registry struct {
	prom *prometheus.Registry

	mu         sync.Mutex
	counters   map[string]prometheus.Counter
	gauges     map[string]prometheus.Gauge
	histograms map[string]prometheus.Histogram
	counterVec map[string]*prometheus.CounterVec
	gaugeVec   map[string]*prometheus.GaugeVec
	histVec    map[string]*prometheus.HistogramVec
}

func NewRegistry() *Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return &Registry{
		prom:       registry,
		counters:   make(map[string]prometheus.Counter),
		gauges:     make(map[string]prometheus.Gauge),
		histograms: make(map[string]prometheus.Histogram),
		counterVec: make(map[string]*prometheus.CounterVec),
		gaugeVec:   make(map[string]*prometheus.GaugeVec),
		histVec:    make(map[string]*prometheus.HistogramVec),
	}
}

func (r *Registry) IncCounter(name string) {
	r.counter(name).Inc()
}

func (r *Registry) AddCounter(name string, delta uint64) {
	r.counter(name).Add(float64(delta))
}

func (r *Registry) SetGauge(name string, value int64) {
	r.gauge(name).Set(float64(value))
}

func (r *Registry) IncCounterVec(name string, labels map[string]string) {
	r.counterVecFor(name, labelNames(labels)).With(labelValues(labels)).Inc()
}

func (r *Registry) AddCounterVec(name string, labels map[string]string, delta float64) {
	r.counterVecFor(name, labelNames(labels)).With(labelValues(labels)).Add(delta)
}

func (r *Registry) SetGaugeVec(name string, labels map[string]string, value float64) {
	r.gaugeVecFor(name, labelNames(labels)).With(labelValues(labels)).Set(value)
}

func (r *Registry) ObserveHistogram(name string, value float64) {
	r.histogram(name).Observe(value)
}

func (r *Registry) ObserveHistogramVec(name string, labels map[string]string, value float64) {
	r.histogramVecFor(name, labelNames(labels)).With(labelValues(labels)).Observe(value)
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.prom, promhttp.HandlerOpts{})
}

func (r *Registry) Render() string {
	families, err := r.prom.Gather()
	if err != nil {
		return fmt.Sprintf("# gather_error %v\n", err)
	}

	var builder strings.Builder
	encoder := expfmt.NewEncoder(&builder, expfmt.FmtText)
	for _, family := range families {
		_ = encoder.Encode(family)
	}
	return builder.String()
}

func (r *Registry) counter(name string) prometheus.Counter {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.counters[name]; ok {
		return metric
	}

	metric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: name,
	})
	r.prom.MustRegister(metric)
	r.counters[name] = metric
	return metric
}

func (r *Registry) gauge(name string) prometheus.Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.gauges[name]; ok {
		return metric
	}

	metric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: name,
		Help: name,
	})
	r.prom.MustRegister(metric)
	r.gauges[name] = metric
	return metric
}

func (r *Registry) histogram(name string) prometheus.Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.histograms[name]; ok {
		return metric
	}

	metric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    name,
		Buckets: prometheus.DefBuckets,
	})
	r.prom.MustRegister(metric)
	r.histograms[name] = metric
	return metric
}

func (r *Registry) counterVecFor(name string, labelNames []string) *prometheus.CounterVec {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := vecKey(name, labelNames)
	if metric, ok := r.counterVec[key]; ok {
		return metric
	}

	metric := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: name,
		Help: name,
	}, labelNames)
	r.prom.MustRegister(metric)
	r.counterVec[key] = metric
	return metric
}

func (r *Registry) gaugeVecFor(name string, labelNames []string) *prometheus.GaugeVec {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := vecKey(name, labelNames)
	if metric, ok := r.gaugeVec[key]; ok {
		return metric
	}

	metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: name,
	}, labelNames)
	r.prom.MustRegister(metric)
	r.gaugeVec[key] = metric
	return metric
}

func (r *Registry) histogramVecFor(name string, labelNames []string) *prometheus.HistogramVec {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := vecKey(name, labelNames)
	if metric, ok := r.histVec[key]; ok {
		return metric
	}

	metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    name,
		Buckets: prometheus.DefBuckets,
	}, labelNames)
	r.prom.MustRegister(metric)
	r.histVec[key] = metric
	return metric
}

func vecKey(name string, labelNames []string) string {
	return name + "|" + strings.Join(labelNames, ",")
}

func labelNames(labels map[string]string) []string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func labelValues(labels map[string]string) prometheus.Labels {
	values := make(prometheus.Labels, len(labels))
	for key, value := range labels {
		values[key] = value
	}
	return values
}
