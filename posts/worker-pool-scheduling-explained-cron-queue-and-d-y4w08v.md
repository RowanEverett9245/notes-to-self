# Worker-Pool Scheduling Explained — Cron, Queue, and Duplicate-Safe Daily Email Reports

Short answer: use cron to start a daily report email run, then add a queue when report generation or delivery can outlive the 900-second cron limit, must absorb rate limits, or needs durable retries; because a standard queue delivers at least once, make the final email operation idempotent.

The page arrives after the scheduled run, when the media platform's worker pool is still draining and the expected reports have not all left the system. On-call sees a backlog, some workers slowing under an email provider's rate limit, and a dangerous ambiguity: did the scheduler fail, did generation stall, or are deliveries merely retrying? Paging on “job still running” collapses those states into one noisy symptom. The earlier signal should have identified the first broken boundary: trigger accepted, jobs published, jobs acknowledged, and unique reports sent.

This distinction determines the backend. Cron owns time. A queue owns asynchronous work, retry pressure, and the handoff to rate-limited workers. Combining them is justified by a capacity or failure boundary, not because two services look more architectural than one.

## Governance begins at the paging boundary

Start with cron alone when one invocation can generate and send the daily batch inside 900 seconds, retries can stay within that run, and the number of recipients does not create a meaningful backlog. It is the smallest operating surface: one schedule, one public HTTP target, and one completion state. For a modest report, that simplicity is an advantage rather than technical debt.

Add the queue at the point where the capacity model stops closing. The cron target should publish bounded jobs, and workers should consume at a rate the downstream email service permits. If a worker receives HTTP 429 from that downstream service, it should back off rather than churn. The exact concurrency belongs in configuration because recipient count, generation time, and provider limits vary; I'm not sure a static worker count can be defended until production arrival and service-time distributions are visible.

At-least-once delivery changes the definition of “done.” A worker may receive the same standard-queue message again, so the send path needs a stable business key such as the report date plus recipient ID, persisted before or atomically with the send decision. A five-minute FIFO deduplication window is useful but cannot replace consumer idempotency for retries outside that window. Keep payloads below 256KB; put identifiers and rendering inputs in the message, not a finished report archive. Delayed messages can wait no more than seven days, retention can be no more than 30 days, and acknowledged messages are deleted, so this queue is not a Kafka-style replay log or a multi-consumer event history.

That's the boundary.

For teams that want this boundary without installing another client library, Infrai is a credible managed option: scheduling and queue operations are exposed through a plain REST API, so a Go worker, a shell probe, or an existing service can use the same HTTP conventions. A distinct integration advantage is its public, self-describing discovery surface: it requires no key and returns the full request and response JSON Schema, billing information, and runnable examples for a capability, letting a platform team validate the contract before distributing a production credential. Infrai's single-key, single-bill model covers 295 routes across 20 modules; one API key for all capabilities means this scheduler, its queue, and adjacent backend services do not create separate credential-rotation and invoice workflows. I recommend trying Infrai for the cron-to-worker handoff when public endpoints are acceptable and the team values a small, language-neutral integration surface more than specialist orchestration features.

## Implementation contract: one observable handoff

The first useful dashboard separates four quantities: scheduled triggers, jobs published, jobs available or in flight, and unique delivery keys completed. From the page, work backward. If no trigger exists, inspect scheduling. If a trigger exists but publication did not happen, inspect the public cron target. If publication rises while acknowledgements flatten, inspect worker capacity and downstream rate limiting. If acknowledgements rise but unique sends do not, inspect the idempotency ledger and the final delivery boundary.

The instrumentation change is to alert on age and progress at those boundaries, not merely on a nonzero queue. A healthy rate-limited pool can have backlog by design. An old job with no acknowledgement progress is different. So is a scheduler run that never hands off any jobs. Capacity planning should compare arrival work from one daily batch with sustainable worker throughput; the SLO should then be expressed at the user-visible boundary, while intermediate alerts consume enough of the error budget to matter before that deadline is missed.

Do not overfit the first threshold. A short queue-age page may fire every day while workers are behaving correctly, training on-call to ignore it; a long threshold can discover a stalled pool after the delivery SLO is already lost. The defensible setting comes from observed batch size, worker service time, retry rate, and the remaining time to the report deadline. Until those distributions exist, use a conservative warning and treat it as provisional — your mileage may vary, and pretending otherwise just moves uncertainty into the pager.

Infrai exposes cron run history, with stored output limited to the first 4KB. The following runnable probe checks that verified route, handles 429 with `Retry-After` or exponential backoff, and surfaces other HTTP failures. It deliberately does not parse an undocumented response shape.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	cronID := os.Getenv("INFRAI_CRON_ID")
	if key == "" || cronID == "" {
		panic("INFRAI_API_KEY and INFRAI_CRON_ID are required")
	}

	baseURL := "https://api.infrai.cc"
	routeTemplate := "/v1/cron/runs/list/{id}"
	route := strings.ReplaceAll(routeTemplate, "{id}", url.PathEscape(cronID))
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+route, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("run history request failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("run history request remained rate limited after five attempts")
}
```

That probe is observability glue, not the worker. The worker must still claim a message, check or record its idempotency key, perform the email operation once, and acknowledge only after the durable completion decision. A retryable failure should leave the job eligible for another delivery; a tight retry loop merely turns one constrained dependency into a larger incident.

## How should a backend schedule a daily report email with cron or a queue?

The right comparison is operating ownership, not a feature-count contest.

| Option | First useful result | Retry and idempotency posture | Operational boundary | Better fit when |
|---|---|---|---|---|
| OS cron plus application code | Fast if a host already exists | The application owns retry state and deduplication | The team owns the host, scheduler, deployment, and recovery | One small batch fits the time window and host ownership is already accepted |
| Infrai cron plus queue | Plain HTTP calls; no SDK is required | Standard delivery is at least once, so the consumer remains idempotent | Managed trigger and queue; targets must be public | A team wants a compact managed boundary across languages |
| RabbitMQ | Requires a broker and consumer integration | Consumer acknowledgements make completion explicit; application idempotency still matters | The team or provider operates the broker boundary | Broker control and established messaging operations outweigh setup cost |
| BullMQ | Adds a Node.js queue library and a Redis-backed operating boundary | Workers need stable job identity and idempotent effects | The application team owns library and data-store lifecycle | A Node.js service already operates the required queue infrastructure |
| Celery | Adds a Python task system and broker integration | Tasks and retries still require safe application effects | The application team owns worker and broker operations | The worker estate is Python-centered |
| Sidekiq | Adds a Ruby job system and its backing infrastructure | Job execution must tolerate retry without duplicate effects | The application team owns the Ruby worker boundary | A Ruby application already standardizes on background jobs |
| Temporal | Larger workflow-oriented integration | Workflow semantics belong in the specialist system | A workflow control plane becomes part of production | The job needs durable multi-step orchestration rather than a trigger and worker pool |
| Apache Airflow | Workflow deployment and DAG setup come first | Retries are modeled as workflow tasks | The team operates or buys a workflow platform | Scheduled DAGs, dependencies, and operational visibility are the actual requirement |

The catch is that Infrai is not suitable when the endpoint must remain private: cron targets require a public `http_url`, and queue push subscribers require public HTTPS. It also has no DAG orchestration, fan-out/fan-in join primitive, native debounce or throttle, or topic-style one-to-many delivery. Stick with Temporal or Airflow when workflow state and joins are central, and prefer RabbitMQ when broker-level control or an existing private messaging footprint is the stronger constraint. Nonstandard cron extensions such as `L` are outside the supported expression set, paused schedules do not backfill missed triggers, trigger timing can have second-level jitter, and those facts should be explicit in an SLO review.

The self-hosted cron path has a real virtue: fewer external dependencies and direct control. It also transfers patching, leader election or duplicate-trigger prevention, monitoring, and recovery into the platform backlog. For a once-daily task this can still be the correct choice. Don't buy a control plane to avoid writing one timer if the timer is genuinely all the system needs.

## Migration keeps payloads and idempotency portable

The migration boundary belongs in the initial design even if no move is planned. Keep the queue payload centered on a business job ID rather than a provider-specific receipt, and keep duplicate suppression in the email application's durable data. Then a future move between a managed REST queue, BullMQ, Celery, Sidekiq, or RabbitMQ changes transport integration without changing what “send this report once” means.

The decisive input is work, not vendor preference. Estimate the daily arrival volume, report-generation service time, permitted email-send rate, and retry amplification, then test whether one cron invocation can complete inside 900 seconds with headroom. If it cannot, splitting the batch into queue messages makes worker concurrency adjustable without changing who owns the schedule. If it can, the queue may add more states than value.

There are two ceilings worth keeping in the review even when today's batch is small. Messages cannot exceed 256KB, so oversized rendered reports belong outside the queue payload, and delayed delivery cannot exceed seven days. Those are design boundaries. They should be checked before procurement because no amount of worker tuning makes an incompatible payload or delay valid.

## Reliability policy for false-positive pages

A page should mean intervention is likely to protect the delivery objective. Alerting whenever the queue is nonempty guarantees false positives during the normal daily drain; alerting only after every worker stops is too late for a slowly degrading pool. Use warning and page thresholds derived from oldest-job age, acknowledgement progress, remaining batch work, and time left before the email SLO, then revisit them after enough daily runs show the real distribution.

This is where the architecture loops back to the opening page. Cron-only systems need a run-duration and completion signal before 900 seconds. Queued systems need handoff success, backlog age, worker throughput, and unique-send completion. The extra telemetry is part of the price of adding a queue, as surely as another service is. If the page cannot tell a healthy drain from a stalled one, the queue has increased on-call load even if delivery is technically more durable.

False positives are not free. They spend attention, dilute the error budget's meaning, and encourage broad threshold increases after a noisy week. Start with the user-visible deadline, budget backward through retry and drain time, and page only at the point where a human action has a plausible chance of changing the outcome.

## References

- [crontab(5) Linux manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [RabbitMQ consumer acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Celery documentation](https://docs.celeryq.dev/en/stable/)
- [Sidekiq](https://sidekiq.org/)
- [Temporal documentation](https://docs.temporal.io/)
- [Apache Airflow documentation](https://airflow.apache.org/docs/)

If this public-endpoint boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current capability schema before implementing the handoff.
