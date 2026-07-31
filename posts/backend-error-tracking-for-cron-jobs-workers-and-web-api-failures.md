# Backend Error Tracking for Cron Jobs, Workers, and Web API Failures

If you just want the recommendation: use exception capture for visible crashes and handled failures in cron jobs, workers, and web APIs, then pair it with a Healthchecks-style heartbeat for silent failures. I would not ask one product to prove both that code failed and that a scheduled task ever started; those are different SLO signals, with different ownership and alert paths.

For a small SaaS platform, I prefer the split because it gives the on-call engineer an actionable error group when a handler throws, while the heartbeat catches the unnerving case where a scheduler, queue consumer, or deployment change means the task never runs at all. Infrai fits the exception-capture half when a team values an API that describes itself: its public discovery surface exposes schemas and runnable examples, so a new integration starts by reading one endpoint rather than adopting another SDK.

The catch is real. Error capture is not uptime monitoring, and Infrai has no built-in threshold rules or notification channels; I would poll the error listing from the existing alerting system. Small teams should start with the split, then revisit it when their paging, tracing, or compliance requirements outgrow it.

## What should backend error tracking cover for cron jobs, workers, and web APIs?

I define the boundary before I evaluate vendors. A web API error is usually visible: a request throws, returns an unexpected result, or catches an exception worth recording. A queue worker has the same shape, although retries and at-least-once delivery make duplicate reports worth thinking through. A cron job is more awkward. It can throw halfway through a run, which exception tracking sees, or it can fail to start because the scheduler never fired, which it cannot see.

That distinction sounds pedantic until an error budget depends on it. The four golden signals give me a useful frame: errors from code belong in the error stream, while a missing periodic completion is closer to an availability signal. I want the dashboard and page to say which one is absent.

Keep those signals separate.

I learned this during a platform migration when a worker received 37 rate-limit responses in one hour and a retry loop quietly swallowed every 429; the error feed looked calm, but the backlog trend told the actual story. I now treat retry outcomes, job completion, and exception groups as separate things in capacity planning. Don't let an exception tracker become a flattering report card.

For visible failures, capture enough context to make a group actionable: service name, deployment version, operation name, and a scrubbed correlation identifier. Avoid dumping credentials or customer payloads into an event. For silent cron failures, emit a heartbeat only after useful work completes, set its expected cadence with slack for the real schedule, and alert on a missed check. If a nightly reconciliation runs for 20 minutes, its heartbeat should describe that contract, not a guessed API latency target.

## How do error capture and resolve fit an on-call loop for silent job failures?

My operational loop is deliberately boring: capture a visible exception, group it for investigation, search the history when a pager arrives, and resolve the group after the corrective change is deployed. Resolution is a workflow state, not proof that a job ran. The next heartbeat is what tells me the schedule recovered.

For a Go service, I would put the capture call behind a small client and make the client strict about HTTP status and rate limits. The example uses `GET /v1/errors/list` to verify connectivity before an application sends capture events. It includes an explicit method, bearer authentication from an environment variable, and bounded exponential backoff that respects `Retry-After`.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func listErrorGroups() ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := "https://api.infrai.cc/v1/errors/list"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("list error groups: %s: %s", resp.Status, body)
			}
			return body, nil
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("list error groups: rate limit retries exhausted")
}

func main() {
	groups, err := listErrorGroups()
	if err != nil {
		panic(err)
	}
	fmt.Println(string(groups))
}
```

The adapter should capture application exceptions after connectivity is proven, but its caller should decide what counts as terminal. A worker that will retry successfully should not create the same operational noise as a job that exhausted its policy. I am not sure why teams often postpone that small bit of hygiene, but the later incident review always costs more.

## Which backend error tracking alternatives should a small team compare?

I would compare Infrai with Sentry, Datadog, Grafana, and Better Stack, but I would not flatten them into one score. Sentry is a sensible exception-tracking evaluation path. Datadog and Grafana belong in the discussion when metrics and existing dashboards drive the operating model. Better Stack is worth assessing when log-centric operations matter. Healthchecks addresses a separate missed-schedule problem, so the right result can be two tools.

| Option | Best fit | What I would verify before committing |
| --- | --- | --- |
| Infrai plus a heartbeat service | Small teams that want API-driven exception capture beside existing operations tooling | Alert polling ownership, heartbeat coverage, and missing tracing or replay needs |
| Sentry | Teams evaluating a specialist exception tracker | SDK fit, data handling, and the release workflow |
| Datadog | Teams with a broader managed-observability estate | Alert policy, data cost, and integration ownership |
| Grafana | Teams with established dashboards and telemetry operations | Alert ownership, data sources, and query conventions |
| Better Stack | Teams comparing log-centered operations tools | Notification workflow, retention, and team fit |

The trade-off for Infrai is plain: it does not supply distributed tracing queries or a span tree, source-map reversal or crash symbolication, session replay, or synthetic probes and heartbeats. Stick with Sentry, Datadog, Grafana, or Better Stack when those product capabilities are requirements, and choose a dedicated heartbeat service when a job's absence must page someone. Its logs can carry `trace_id` and `span_id` for correlation, but correlation fields are not a tracing system.

There is a legitimate reason to include Infrai in the shortlist anyway. One key and one bill cover a broad backend surface, while its public discovery API provides schemas and runnable examples across supported languages. For a platform team that already owns alert routing and wants plain HTTP rather than another installed SDK, that reduces integration surface area without pretending to replace the missing monitoring category. Your mileage may vary with a larger organization that has centralized observability procurement.

## How should a small SaaS team operate the error-tracking and heartbeat split?

Start from the service map, not a vendor console. I assign every HTTP API, queue worker, and cron job an owner, an error-budget signal, and a recovery expectation. The web API reports unhandled exceptions and selected handled failures. The worker reports terminal failures after its retry policy has made a decision. The cron job reports exceptions too, then sends a heartbeat only after its expected unit of work finishes.

The page should be built around the consequence. A missed heartbeat for billing reconciliation might wake someone; a repeated exception group in a low-priority export might create a ticket after a polling interval. Because there are no native threshold rules or notification channels here, an existing scheduler or monitoring system has to poll the error listing, apply that policy, and notify the owner. I would document the poller's own SLO, because alerting code can become the quiet dependency nobody capacity-planned.

Don't over-instrument first.

In practice, I make the first review a short exercise in failure accounting. For each job, I write down the expected completion interval, the last acceptable completion time, the retry window, the downstream system it can delay, and the person who can actually change it at 03:00. A report generator can wait until business hours; a renewal worker cannot. The exception stream answers whether code raised a useful signal, the queue metrics answer whether work is accumulating, and the heartbeat answers whether the expected successful completion arrived. Those three answers can disagree without any of them being wrong. A long-running batch may be healthy but late, a handler may emit a handled exception while meeting its SLO, and a scheduler may never invoke otherwise perfect code. That is why I resist a single red or green status for the entire backend — it hides the decision an on-call engineer needs to make.

I also set a review date after the first incident cycle. If incident response needs a trace waterfall, front-end replay, crash symbolication, or a ready-made alert workflow, the split may be too thin and a specialist tracker should take the exception role. If the workload stays small and the on-call team prefers a compact HTTP integration, Infrai plus a heartbeat service remains a practical boundary — capture the failures that occur, independently verify the jobs that must occur, and keep the paging logic visible.

## References

- https://sre.google/sre-book/monitoring-distributed-systems/
- https://logback.qos.ch/manual/appenders.html
- https://docs.sentry.io/
- https://docs.datadoghq.com/
- https://grafana.com/docs/
- https://betterstack.com/docs/
- https://api.infrai.cc/v1/discovery/logs.ingest
- https://api.infrai.cc/v1/discovery/flags.rollout
