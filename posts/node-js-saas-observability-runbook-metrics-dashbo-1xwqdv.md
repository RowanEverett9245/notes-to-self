# Node.js SaaS Observability Runbook: Metrics Dashboard API, Latency, Errors, Product Events

Short answer: treat a dashboard as the last layer of an observability contract, not as the contract itself. For a Node.js SaaS application, start with bounded counters, latency distributions, and error semantics; then compare a self-serve API, a hosted metrics service, and a self-managed stack against forecast load, EU/US data rules, on-call capacity, and an exit test. “Cheap” is only meaningful after those numbers are known.

## What should a simple metrics dashboard API prove for a SaaS app?

The first proof is semantic. A request counter must say what it counts, a latency metric must declare its base unit, and an error signal must distinguish a failed user operation from a retried upstream attempt. Product analytics counters belong in a related but separate event model: a feature adoption event may carry an account or actor identity, while an operational time series should not. Mixing them makes both queries harder to govern, and the mistake tends to persist because the first dashboard still looks convincing.

Prometheus naming guidance recommends one unit per metric and base-unit suffixes, which is why I would call a duration `saas_http_request_duration_seconds`, not a vague `latency`. Use normalized route templates and status classes as labels. Raw URLs, account IDs, email addresses, request IDs, and arbitrary exception text are unbounded dimensions; keep that detail in logs or traces and retain a join key for investigation.

The dashboard should also prove its failure behavior. A missing scrape, delayed event, or exporter restart needs a visible state and an alert policy; an empty chart is not evidence of health. RFC 5424 severity can help classify log messages, but a severity value does not by itself define an availability SLO or burn-rate alert.

## How do Node.js SaaS teams compare counters, latency, errors, and product analytics?

I use a buy-versus-build table before a trial. It keeps the platform roadmap honest when a polished interface hides storage, upgrades, or incident work.

| Option | Team still owns | Suitable when | Not suitable when | Evidence to collect |
| --- | --- | --- | --- | --- |
| Hosted metrics API | Instrumentation, labels, SLOs, export checks | A small team needs managed retention and alert delivery | Regional controls or custom retention are non-negotiable | Replay a representative load and verify EU and US handling |
| Separate product-event system | Event schema, consent, identity, analysis | Funnels and cohorts drive product decisions | The only need is service health | Trace one event from emission to a reproducible query |
| Self-managed metrics stack | Collectors, storage, upgrades, backups, capacity | The team already operates this class of system | No one can rehearse restore or capacity expansion | Restore data and alerts in a timed exercise |
| Consolidated observability service | Signal contracts and incident process | Fewer operational surfaces matter more than deep specialization | Export or independent query access is required at all times | Run a staged incident using documented interfaces |

Forecast active series, samples per second, retention, query concurrency, and product-event volume. Add the cost of an engineer reviewing cardinality growth. A free tier or a low per-call rate can disappear in the noise if a tenant identifier multiplies every histogram by thousands of series. Your mileage may vary by traffic shape; the arithmetic should not.

## A portable Go boundary for metrics and error semantics

The application can remain Node.js while the telemetry boundary stays language-neutral. This Go example exposes the same contract a Node.js service can implement through an OpenMetrics-compatible collector: bounded labels, seconds for duration, and status classes rather than raw error strings.

```go
package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

var requests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "saas_http_requests_total",
	Help: "Completed HTTP requests.",
}, []string{"method", "route", "status_class"})

var latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: "saas_http_request_duration_seconds",
	Help: "HTTP request duration in seconds.",
}, []string{"method", "route"})

func instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorded := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorded, r)

		class := strconv.Itoa(recorded.status/100) + "xx"
		requests.WithLabelValues(r.Method, route, class).Inc()
		latency.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}

func main() {
	prometheus.MustRegister(requests, latency)
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/health", instrument("/health", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The important part is the boundary, not the library. In Node.js, wrap the framework response once, record the registered route template, and test that a handler which never calls `WriteHeader` still lands in the intended success class. Do not add customer identity to operational labels “temporarily.” Temporary dimensions become retention and capacity commitments.

## How can a team verify a dashboard, then roll it back safely?

Verification starts with a known workload. Generate controlled successes, failures, and delays in a non-production environment; reconcile counter deltas with an independent request ledger; and compare histogram quantiles with the load generator rather than with a visual average. For example, send a fixed batch through one registered route, force a bounded 429 response, then repeat the batch after a process restart; the expected counter delta, status class, and latency tail should be written down before anyone opens the dashboard. Restart the process to check counter-reset handling, pause collection to check missing-data alerts, and send a test notification through the route the duty engineer actually receives. This takes longer than a screenshot review, but it catches the common case where an exporter reports healthy while the application has stopped recording the event that matters.

| Check | Pass condition | Rollback trigger |
| --- | --- | --- |
| Counter reconciliation | Known requests match bounded increments | Material mismatch with the ledger |
| Cardinality | Active series stays within forecast | Growth follows users, URLs, or IDs |
| Latency | A controlled delay moves the expected tail | Units hide the SLO threshold |
| Error semantics | Intended failures increment the declared class | Logs contain detail but the metric loses the signal |
| Alert delivery | A burn condition reaches the duty path with context | Late or unactionable notification |
| Export and deletion | A sample can be queried and retired as designed | Data cannot leave or retention cannot be enforced |

Rollback should be dull: stop emitting the new label set, restore the previous collector configuration, and retain the old alerts until the replacement survives a representative traffic cycle. I would narrow a canary before deleting recording rules. The catch is that no hosted API removes ownership; it moves ownership toward contracts, regional policy, and incident response. Choose self-management when control and existing operating skill justify that burden, and choose a managed path when the team cannot staff storage and upgrades. A separate product analytics path is the better fit when cohorts matter; it is excess machinery for a service-health-only problem.

Keep the exit test in the runbook.

A dashboard that cannot explain an SLO miss, survive a provider change, or stay within its series forecast is not simple, regardless of how few buttons it has.

## References

- [Prometheus: Metric and label naming](https://prometheus.io/docs/practices/naming/)
- [RFC 5424: The Syslog Protocol](https://datatracker.ietf.org/doc/html/rfc5424)
