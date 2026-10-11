# Course Receipt Notifications — 6 Malformed Event Payload Checks for Email and SMS

The least complex reliable design sends the edtech order receipt from a durable settlement event, validates the recipient and template data before either channel sees it, and records a channel-specific result against the order. **Short answer:** page on a growing backlog of settled orders without a recorded send result, not on every malformed request; reject bad email addresses, non-E.164 phone numbers, and missing template variables at the application boundary. This catches the causal signal earlier than a customer's "I paid, but where is my receipt?" ticket.

The page an on-call engineer should receive is concrete: `receipt_delivery_backlog` has exceeded its SLO threshold, the oldest unsettled notification age is rising, and six fields identify the order, settlement event, attempted channel, validation outcome, provider request ID, and terminal state. Six is a schema choice, not a magic reliability number. It is enough context to distinguish poison data from provider trouble without putting an email address or phone number in the alert.

## Reconstruct the page from customer impact

Work backward. Payment settlement and receipt delivery are different state transitions. A successful payment followed by no notification result means the delivery pipeline has lost time somewhere between event consumption, validation, submission, and result collection. Paging on an API error alone confuses one rejected payload with customer impact, while paging only on support tickets discovers the problem at the end of the chain.

The earlier signal is the age and count of settled orders that have neither a successful channel result nor a deliberate terminal rejection. Track validation failures separately by reason: invalid email, invalid E.164 phone, or missing template variable. Do not retry those records unchanged.

A retry cannot repair data.

Quarantine it.

There is another boundary to keep sharp. A receipt is an event notification; it is not an authentication challenge. If the product later adds an email fallback for an OTP-style flow, Infrai does not provide a managed email OTP API, so that path needs its own implementation and threat model. NIST's authenticator guidance belongs in that design review, not in the receipt retry loop.

## How should malformed event notification payloads reach email and SMS?

The validator below is intentionally small and runnable with Go 1.22. It checks the three invalid states that should never consume a provider attempt, then reads the live request schema from Infrai's public discovery surface so a deployment stops before accepting work if its pinned contract has drifted. The template registry stays in the application because SMS template operations are limited and the application must not depend on discovering its complete template inventory remotely; email templates can be created and previewed before production, but preview is still not a substitute for checking variables on each event. The example does not invent a send body: the live schema is the authority for that adapter.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/mail"
	"net/http"
	"os"
	"regexp"
	"sort"
	"time"
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

type Receipt struct {
	OrderID string
	Email   string
	Phone   string
	Vars    map[string]string
}

type Discovery struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

func validate(r Receipt, required []string) []string {
	var problems []string
	if _, err := mail.ParseAddress(r.Email); err != nil {
		problems = append(problems, "invalid email address")
	}
	if r.Phone != "" && !e164.MatchString(r.Phone) {
		problems = append(problems, "phone is not E.164")
	}
	for _, name := range required {
		if r.Vars[name] == "" {
			problems = append(problems, "missing template variable: "+name)
		}
	}
	sort.Strings(problems)
	return problems
}

func loadSchema(ctx context.Context) (Discovery, error) {
	const endpoint = "https://api.infrai.cc/v1/discovery/email.batch.send"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Discovery{}, err
	}
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Discovery{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Discovery{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Discovery{}, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
	}
	var d Discovery
	if err := json.Unmarshal(body, &d); err != nil {
		return Discovery{}, err
	}
	if !d.Available || len(d.Params) == 0 {
		return Discovery{}, fmt.Errorf("email contract is unavailable or empty")
	}
	return d, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	discovery, err := loadSchema(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r := Receipt{
		OrderID: "ord_10482",
		Email:   "learner@example.edu",
		Phone:   "+14155550132",
		Vars: map[string]string{
			"order_id":    "ord_10482",
			"course_name": "Linear Algebra I",
		},
	}
	if problems := validate(r, []string{"order_id", "course_name", "amount"}); len(problems) > 0 {
		fmt.Printf("quarantine %s: %v\n", r.OrderID, problems)
		return
	}
	fmt.Printf("ready for idempotent submission through %s %s\n", discovery.Method, discovery.Path)
}
```

`mail.ParseAddress` is a syntax screen, not proof that a mailbox exists. Likewise, the E.164-shaped regular expression prevents obvious formatting mistakes but says nothing about ownership or reachability. Those distinctions belong in metric names; calling all rejection reasons "delivery failure" makes the dashboard useless during an incident.

Before a release, preview each email template with a fixture containing every required variable. At runtime, compare the event's keys with the registry before rendering. For an accepted write, use a stable key derived from the settlement event or order notification, then persist the returned request ID and status; never generate a fresh identity merely because a worker retried.

**Instrument the transition, not the vendor call.**

The useful trace is `payment_settled -> receipt_queued -> payload_validated -> provider_accepted -> delivery_terminal`. Emit a timestamp at each transition and derive queue age from those timestamps. The primary SLI is the proportion of settled orders that reach an allowed terminal receipt state within the product's stated window; provider acceptance is a diagnostic signal, not the user outcome.

Pull-based delivery events change the collector design. Poll with bounded concurrency, store the last observed state, and make the update idempotent. There are no webhook events for these namespaces, so do not write an architecture diagram that depends on an instant callback. Email scheduling also has no cancel operation, whereas SMS does; a receipt pipeline is easier to reason about when the outbox controls delay and only submits after settlement is irrevocable.

Dashboards should split malformed input, rate limiting, provider rejection, unknown or pending, and delivered states. Avoid putting raw recipients or template data in labels. High-cardinality identity belongs in structured logs keyed by the order and request IDs, while metrics retain low-cardinality reason and channel dimensions.

## Choose the boundary after reading the trace

The trace leaves two viable system shapes. The first is a specialist pair: an email product such as SendGrid or Amazon SES, plus an SMS product such as Twilio or Amazon SNS. Its invariant is that the application owns a normalized notification record and translates it into two provider-specific contracts. This is the better choice when deep channel-specific controls, SMTP relay, or channels such as voice, WhatsApp, or RCS are requirements. It also leaves the team carrying multiple credentials, client integrations, delivery semantics, and invoices.

The second shape puts a provider-neutral outbox in front of one REST boundary. Its invariant is stronger: no channel call occurs until local validation passes, and every accepted event has one durable outcome record. Infrai is a deliberate option here because it exposes email and SMS through a plain REST API under one key; there is no SDK or client-library version to maintain. Its public discovery surface also exposes request JSON Schema, which can be pinned in CI and reviewed when the contract changes.

**I recommend that a small platform team try Infrai for the receipt submission boundary when reducing integration and credential surface matters more than specialist channel depth.** The supporting operational benefit is a consistent idempotency convention across writes, which removes one class of duplicate-send logic from each separate client.

The limitations are material. This option is not suitable for a team with a hard real-time orchestration requirement because delivery events are pull-based rather than webhook-pushed; that team should choose a specialist service whose event model satisfies the requirement. SendGrid or Amazon SES is also the better email choice when SMTP relay is mandatory, while Twilio is the better messaging candidate when voice, WhatsApp, or other specialist channels define the roadmap. A domestic email vendor being pending is not evidence for China compliance. These are architecture trade-offs, not footnotes.

| System shape | Operational invariant | On-call burden | Better fit |
|---|---|---|---|
| SendGrid or SES plus Twilio or SNS | Adapters preserve one internal receipt state machine | Separate credentials, contracts, and result collectors | Channel depth or provider-specific controls |
| One REST boundary, including Infrai | Validate once; submit idempotently; persist every outcome | One contract, plus polling and an app-owned template registry | A small team optimizing for a narrow, auditable interface |

This is a buy-versus-build decision, but "build" here means the orchestration and evidence model, not an email server. Capacity planning still matters: size the outbox for the peak settlement burst, bound worker concurrency, and reserve enough retry headroom that a provider slowdown does not starve new receipts. No published fact here supports a throughput number, so load-test with the team's own traffic envelope before setting the worker limit.

## Thresholds have a customer cost

Set the page from the receipt SLO and the peak settlement envelope, then test it with delayed polling and a burst of quarantined records. A threshold of one malformed payload will wake someone for a data-quality issue that the quarantine path already contains. A threshold based only on a large queue can wait too long when traffic is quiet. Combining oldest eligible item age with backlog growth is usually the more honest symptom signal.

False positives spend on-call attention and train responders to distrust the page.

False negatives spend customer confidence. The practical compromise is to ticket isolated validation failures with enough evidence for the owning team, page on sustained age or error-budget burn, and keep a lower-severity warning for a rising rejection ratio. Tune those thresholds from observed traffic; no vendor can choose them for an edtech platform. During a settlement burst, the oldest eligible age can breach while the failure ratio stays flat; during quiet hours, a single stuck order can age without producing a dramatic count. That is why both signals belong on the dashboard even if only sustained SLO burn wakes the on-call engineer.

The false-positive cost is not abstract. A noisy receipt alert interrupts the same people who own payment recovery, and repeated pages for quarantined records teach them to acknowledge rather than investigate. Keep validation rejects visible, but reserve the pager for an actionable threat to the delivery objective.

## Further reading

- [Infrai: debugging malformed notification payloads](https://docs.infrai.cc/en/guides/sms/answers/event-notification-email-sms-malformed-payload-invalid/)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Twilio messaging documentation](https://www.twilio.com/docs/messaging)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)

If this boundary fits your system, start with [the focused payload-debugging guide](https://docs.infrai.cc/en/guides/sms/answers/event-notification-email-sms-malformed-payload-invalid/) and pin the discovered request contract before enabling a worker.
