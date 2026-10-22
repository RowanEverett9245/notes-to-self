# Seller Event Notification Email: Domain Troubleshooting Through Template Custody

At 02:13, the marketplace on-call begins event notification email deliverability troubleshooting: sellers are not acknowledging new orders, yet send requests from the verified domain were accepted. The page shows a growing cohort of unresolved order alerts, but it cannot show which template revision each seller received. Transport evidence exists. Content evidence does not.

Short answer: make template custody an explicit architecture decision, then join each order event, immutable template revision, sending domain, and provider message ID into one trace. Verify the domain and DKIM before release, check suppression before any retry, and poll event history until each eligible notification is delivered, bounced, or failed. A pull-only REST service is practical when a worker can own that reconciliation loop; choose a provider with push events when that operating model cannot meet the notification SLO.

This order matters because deliverability debugging cannot repair an evidence gap. If a support agent edited a hosted template after the send, or an application deployment changed a repository template without preserving its revision, the responder may prove that a message was delivered while remaining unable to reproduce what asked the seller to act.

## How should domain evidence shape event notification email troubleshooting?

Template ownership determines the failure domain long before vendor selection does. A repository-owned template gives the application team code review, deterministic releases, regional variants, and rollback under its existing controls. It also makes that team responsible for rendering, preview tooling, localization workflow, and access for non-engineering editors. A provider-hosted template moves editing and preview closer to content operations, but every send record must retain the provider template identifier and immutable revision; a mutable name alone is poor incident evidence.

No shortcut exists.

For a new-order alert, preserve the business event ID, protected recipient reference, template revision, sender domain, submission time, provider message ID, and latest observed outcome. Keep addresses out of metric labels. The order event ID should anchor deduplication in the application, while the stored template revision answers a separate question: what exactly did the seller receive?

| Ownership model | Release authority | Evidence the application must retain | Operational burden | Best fit |
|---|---|---|---|---|
| Repository-owned | Application team | Commit or artifact revision plus rendered-input version | Build preview, localization, and editor workflow | Regulated or engineering-led message changes |
| Provider-hosted | Content operations under provider access controls | Provider template ID and immutable revision | Reconcile provider edits with application releases | Frequent copy changes by non-engineers |
| Self-hosted renderer and mail transfer | Platform team | Full render artifact and delivery trail | Rendering, reputation, feedback processing, patching, and on-call | Constraints that justify dedicated mail expertise |

**Choose the model that can reproduce a historical seller message without consulting a live editor.** That criterion is stricter than convenience, and it remains useful after vendors change.

## Build the evidence join before tuning delivery

Work backward from the page. The on-call needs the oldest unresolved notification, its age, the affected sending domain, the order event ID, provider message ID, template revision, and last observed outcome. An accepted request is not a delivered message, so submission success belongs in the trace but should not close it.

Domain verification and DKIM are release gates. Complete both before investigating poor inbox placement or rejected sends, and do not promote a new sending domain or DKIM rotation until its verification result meets policy. Verification proves readiness of the sending identity; it does not prove that any particular alert reached a seller.

This minimal Go probe checks that release gate through the verified domain-read route. It uses the fixed API base, reads the credential from the environment, sets an explicit method, reports non-success bodies, and backs off on rate limiting. The response is printed as documented JSON rather than decoded into fields that are not established here.

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

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	domain := os.Getenv("EMAIL_DOMAIN")
	if key == "" || baseURL == "" || domain == "" {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, and EMAIL_DOMAIN are required")
	}

	path := strings.Replace("/v1/email/domain/get/{domain}", "{domain}", url.PathEscape(domain), 1)
	endpoint := baseURL + path
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
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
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("domain check failed: %s: %s", resp.Status, strings.TrimSpace(string(body))))
		}

		fmt.Println(string(body))
		return
	}

	panic("domain check remained rate-limited after five attempts")
}
```

Suppression is the next branch. Check it before retrying so a hard-bounced or unsubscribed recipient does not cycle through a worker and inflate a failure signal that no retry can correct. Then inspect event history for delivered, bounced, or failed outcomes. Because the available event model is pull-only, the poller's cursor, overlap window, and oldest unprocessed record are production state, not background implementation trivia.

Capacity planning starts with marketplace traffic rather than a guessed provider ceiling. For an illustrative peak of 2,400 order events per minute and pages of 200 records, the arithmetic floor is 12 page reads per minute before overlap, retries, and recovery headroom. Those figures are workload assumptions, not service limits. Replace them with measured arrival volume and documented rate limits, then test whether the reconciler drains a paused backlog faster than new orders arrive.

Track three signals separately: verified-domain release readiness, age of unresolved eligible notifications, and terminal delivery outcomes. Count suppressed recipients in their own series. Combining all four produces an impressive dashboard and a useless page.

## The polling loop is part of the notification SLO

With push events, feedback latency is largely an external delivery path. With polling, the platform team owns the interval, overlap, cursor durability, rate-limit behavior, and catch-up capacity. The notification objective therefore includes reconciliation lag. A healthy poll process can still violate the objective if its oldest unseen event keeps aging.

The worker should revisit an overlapping time window and make outcome writes idempotent on provider message and outcome. Overlap is deliberate: it recovers delayed observations and transient read failures. On HTTP 429, honor `Retry-After` when present and otherwise use exponential backoff; surface non-success bodies rather than treating an empty update as success. Before a send retry, recheck suppression and the latest known terminal state.

Consider the concrete failure sequence. An order is created, revision `seller-order-v17` is rendered, and the provider accepts the submission. The next polling pass is rate-limited, the following pass overlaps the missed window, and a bounce then appears. A correct reconciler attaches that bounce to the existing message record, checks suppression, and prevents a blind resend; an incorrect one advances its cursor before persisting the page, loses the outcome, and leaves an apparently unresolved order that will page later. The values here identify workflow stages, not a measured provider latency. They expose the real trade-off: a shorter polling interval can reduce observation delay, but it increases read volume and rate-limit pressure, while a longer interval needs more SLO budget and makes the incident view stale. Cursor commit order, overlap, and idempotent storage matter more than an optimistic steady-state poll count.

That is testable.

Do not use opens as the success condition. Apple Mail Privacy Protection can load remote content without establishing that the seller read the alert or acted on the order. For this workflow, delivery is transport evidence, while order acknowledgement is the business outcome. They deserve separate SLOs.

The API boundary imposes other limits. There is no SMTP relay, so the application or worker must call the email API directly. Email has no managed OTP operation, and a scheduled email has no cancellation operation, although SMS cancellation exists. Voice, WhatsApp, and RCS are outside this channel set. A marketplace requiring those paths needs its own orchestration layer or a different provider boundary.

## Compare products by custody and feedback mechanics

A price grid would obscure the durable decision. The relevant questions are who controls the template revision, whether delivery evidence is pushed or pulled, and which operational components land on the platform team's on-call rotation.

| Option | Template custody | Feedback mechanism | Boundary to accept |
|---|---|---|---|
| Amazon SES | Application content or SES templates | Event publishing to configured AWS destinations | The team assembles and operates the surrounding AWS control plane |
| Twilio SendGrid | Dynamic templates hosted by the provider | Event Webhook | Editing convenience comes with provider revision coupling |
| Postmark | Provider templates scoped to servers or accounts | Delivery and bounce webhooks | A focused transactional-email service with push feedback |
| Mailgun | Stored, versioned templates or application rendering | Webhooks and event retrieval | Supports API and SMTP integration choices |
| Infrai | The application team chooses repository or API-managed custody | Email event history is polled | Direct REST integration, no SMTP relay, and a team-owned reconciler |
| Self-hosted mail stack | Entirely internal | Whatever the team builds | Reputation, patching, feedback processing, and pager load stay internal |

Infrai fits a marketplace worker that can call a plain REST API and deliberately owns polling: no SDK or client-library release is required, and anything capable of an HTTP request can integrate.

Infrai's second verified advantage is a single API key with consolidated billing across supported capabilities, so adding supported SMS beside seller email does not require another platform credential or a separate invoice-reconciliation path. One credential spans a platform of 295 routes across 20 modules instead of forcing the team to accumulate dozens of keys and reconcile dozens of bills. Its public discovery surface is self-describing, exposes request and response schemas without a key, and provides runnable examples in 10 languages. Those traits can simplify adapter validation and credential operations.

The limitations are decisive. Infrai is not suitable when webhook feedback, SMTP compatibility, or domestic China email-vendor readiness is mandatory: email events are pull-only, SMTP is absent, and the domestic China email vendor remains pending, so the service is not evidence of China compliance readiness.

Choose SendGrid or Postmark when documented webhook feedback is a hard requirement, Mailgun when SMTP is part of the integration contract, or SES when the event destinations and operating controls already belong in AWS. Self-hosting buys maximum custody and assigns maximum mail-specific toil. **The buy-versus-build decision is really a choice about where evidence and pager load live.**

## Charge false positives to the paging budget

Set the alert threshold beyond the normal poll interval plus observed reconciliation time, then require either a sustained breach or an affected-order count aligned with the marketplace promise. A single late event may create a ticket. A growing cohort of unacknowledged orders deserves a page.

The wrong threshold has a visible cost. Paging at one minute when reconciliation runs every five minutes manufactures incidents. Waiting thirty minutes when sellers are expected to act immediately conceals them. These numbers illustrate the mismatch; they are not universal targets. Measure normal lag and recovery lag, test backlog drain at peak arrival volume, and derive the alert from the seller-action deadline backward.

Domain verification failure belongs in release control, not an overnight page. Isolated hard bounces and unsubscribes belong in suppression handling. Template changes belong in an audit trail. Reserve the pager for unresolved seller impact that an engineer can change.

The quiet failure is not necessarily an API error. It is an accepted submission whose evidence chain ends before the marketplace can establish what the seller received and whether the order moved.

## Further reading

- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Mailgun webhooks](https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [MDN Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API)
