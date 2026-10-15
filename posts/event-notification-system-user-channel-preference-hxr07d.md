# Event Notification System: User Channel Preferences for Gaming Report Attachments

A generated match report is useful only if the notification pipeline respects the player's latest channel choice at the moment of delivery. **Short answer:** store preferences and suppressions as versioned facts, resolve them into one immutable delivery plan, attach the report only to email, and recheck global suppression immediately before each provider call. SMS should carry a short notice or an authenticated report link, not inherit email's attachment semantics.

That choice adds one read near dispatch, but it closes the dangerous gap between “the user opted out” and “the queued job was already authorized.” For a platform team, that integration boundary matters more than a convenient SDK: consent changes, generated files, retries, and provider feedback must converge on a state machine that can be tested without a live gateway.

## How should an event notification system enforce user channel preferences?

Consider the bounded failure scenario, without inventing a war story: a game server finishes a post-match report, the notification worker records that both channels are enabled, and the player then sends an SMS opt-out before the worker reaches the carrier. A queue payload containing `sms_allowed: true` is now stale. Retrying it faithfully repeats the wrong decision.

The invariant is stricter: **a queued event requests communication; it does not grant permission to send it.** The worker must evaluate the current preference plus the current channel suppression at dispatch. CTIA's messaging principles describe consumer opt-in and opt-out expectations for messaging programs. Email has a parallel operational need even when the source of suppression differs: an explicit unsubscribe, a complaint, or an address-level delivery failure can make an older plan unsafe to replay.

The queue is not consent.

Keep three records separate. A preference says which channels a player wants for a class of event. A suppression says a destination must not be used for that channel. A delivery attempt says what the system tried, under which policy version, and what happened. Combining them into a single mutable “notification status” field destroys the evidence needed to explain a retry or an opt-out race.

Do not delete consent history. Append transitions with an effective timestamp and a monotonically increasing version; restrict access because destinations and consent events are sensitive operational data. The dispatch audit record should retain identifiers and decision reasons, while logs and metrics should avoid raw email addresses, phone numbers, attachment bytes, and signed links.

## Build one decision point, then keep transport adapters dull

The planner accepts an event, the player's current preferences, and active suppressions. It emits zero or more channel-specific intents. An email intent references the generated report object and its content metadata; an SMS intent contains no attachment and can reference a short-lived, authenticated download route if product policy permits one. The adapters translate those intents into transport requests, but they do not reinterpret consent.

Here is the preventative path in Go. The interface is deliberately small so the same tests can exercise a self-hosted SMTP adapter, a managed email API, or an SMS gateway without embedding any provider's response model in policy code.

```go
package notify

import (
	"context"
	"errors"
	"fmt"
)

type Channel string

const (
	Email Channel = "email"
	SMS   Channel = "sms"
)

type Event struct {
	ID, PlayerID, ReportObjectKey string
}

type Decision struct {
	EventID, PlayerID, PreferenceVersion string
	Channels                             []Channel
}

type ConsentStore interface {
	Plan(context.Context, Event) (Decision, error)
	Suppressed(context.Context, string, Channel) (bool, error)
}

type Sender interface {
	SendEmailReport(context.Context, Event) error
	SendSMSNotice(context.Context, Event) error
}

func Dispatch(ctx context.Context, event Event, store ConsentStore, sender Sender) error {
	plan, err := store.Plan(ctx, event)
	if err != nil {
		return fmt.Errorf("plan delivery: %w", err)
	}

	for _, channel := range plan.Channels {
		blocked, err := store.Suppressed(ctx, plan.PlayerID, channel)
		if err != nil {
			return fmt.Errorf("recheck %s suppression: %w", channel, err)
		}
		if blocked {
			continue
		}

		switch channel {
		case Email:
			err = sender.SendEmailReport(ctx, event)
		case SMS:
			err = sender.SendSMSNotice(ctx, event)
		default:
			err = errors.New("unsupported channel")
		}
		if err != nil {
			return fmt.Errorf("send %s for event %s: %w", channel, event.ID, err)
		}
	}
	return nil
}
```

The example omits storage transactions and idempotency intentionally; those belong around the interfaces, not as comments pretending to implement them. In production, claim a channel attempt with a unique key such as `(event_id, player_id, channel, policy_version)` before making the external call. A duplicate queue delivery then finds the existing attempt instead of producing a second message. If policy changes create a new version, the planner can make a fresh, explainable decision.

There is still an unavoidable race between the last suppression read and the external send. Reduce that window, serialize opt-out updates and attempt claims per destination where the datastore supports it, and make inbound opt-outs high-priority work. Do not claim impossible exactly-once delivery across a database and an external transport. The defensible target is idempotent intent handling, a narrow authorization window, and complete evidence for reconciliation.

The race remains.

## Treat the attachment as a capacity problem

Generated reports change the load shape. A worker that fetches a complete file into memory before constructing MIME can multiply peak memory by concurrency, and retries may fetch the same object again. Stream the report from object storage into the MIME encoder when the selected email transport permits streaming; otherwise set a conservative worker concurrency from the largest accepted encoded message, process overhead, and memory limit. MIME transfer encoding can expand binary content, so capacity tests should measure the encoded message rather than only the source file.

Start with a budget, not optimism. For example, if the product accepts reports up to 8 MiB, run load tests at that boundary and with a mixed distribution, then choose concurrency from observed resident memory and the worker's restart threshold. Eight workers is a test input, not a universal recommendation. The SLO should distinguish planning latency, report-generation latency, queue age, and provider acceptance latency; a single end-to-end average hides the queue that is consuming the error budget.

Measure the encoded bytes.

Backpressure is expected. If generation outpaces delivery, keep the object durable, expose queue age by channel, and stop admitting optional report jobs before transactional notifications lose their latency budget. Expire generated objects according to the product's retention policy only after terminal delivery state and retry windows are accounted for.

## Choose the integration boundary before choosing a transport

The buy-versus-build decision is mostly about ownership of the last mile. It should not move preference evaluation into a vendor template or make a vendor webhook the only suppression ledger.

| Boundary | Integration effort | On-call burden | Lock-in surface | Best fit |
|---|---|---|---|---|
| Self-hosted mail transfer plus direct carrier work | Highest: reputation, feedback, routing, and carrier integration stay in-house | Highest; the team owns more failure domains | Lower API lock-in, higher protocol and operations burden | Teams with unusual routing controls and staffed messaging operations |
| Managed email and SMS transports behind local adapters | Moderate: two adapters and two feedback normalizers | Shared with transport operators | Provider request and event schemas | Most teams that want local policy ownership without operating the last mile |
| One communications gateway for both channels | Lowest initial adapter count | Concentrated dependency and escalation path | Gateway policy, schema, and routing | Small teams willing to trade portability for fewer integrations |

The table is not a ranking. A platform with two on-call engineers should price the pager load of self-hosting, while a regulated environment may value local routing control enough to accept it. In every case, keep the canonical preference, suppression, and attempt records under application control; adapters may cache or mirror suppression, but inbound provider feedback must be normalized into that ledger. The main limitation of this design is the coordination it pulls into the application: the team owns policy migrations, the consent datastore, reconciliation, and an audit model even when transports are managed. It is not suitable for a prototype whose messages are limited to a closed internal test group and whose report delivery can be stopped manually; a single gateway with its own preference controls may be the more proportionate alternative there. At the other extreme, direct carrier and mail-transfer operation is a poor default for a small platform team because integration effort and on-call surface expand together. These are real trade-offs, and the right boundary changes when staffing or control requirements change.

Instrument outcomes with bounded labels: channel, event class, decision reason, policy version, and normalized result category. Never label metrics with player IDs or destinations. Alert on sustained queue-age SLO burn, suppression-processing lag, and abnormal shifts in permanent failures; raw failure counts scale with traffic and make poor paging signals.

## Test the transitions, not the happy-path SDK call

The highest-value tests move state between planning and dispatch. Plan both channels, insert an SMS suppression, then assert that only email reaches the fake sender. Deliver the same queue item twice and assert one claimed attempt per channel. Change an email preference after generation but before dispatch. Feed duplicate and out-of-order feedback events into the normalizer. Verify that an unavailable consent store fails closed rather than guessing from a stale queue field.

One test should build a real MIME message and parse it again, checking media type, filename handling, and decoded bytes against the generated report. Keep that fixture free of real player data. Another should exercise an SMS notice containing the authenticated link format, including expiration and authorization at download; possession of a guessed object key must never be enough.

Test the refusal path first.

This advice does not apply unchanged to emergency or legally mandated messages, where the authority to contact a user may come from a separate policy rather than ordinary preferences. Model those as distinct event classes with reviewed rules and audit evidence. It also does not mean every suppression is permanent: scope, reason, source, and effective interval should be explicit so a channel can be restored only through an authorized transition.

The release gate is plain: no transport call without a current decision, no attachment bytes in SMS, no retry without an idempotency claim, and no provider feedback left outside the canonical ledger. That gives the platform team a stable integration surface while transport choices change underneath it.

## Sources

- CTIA, Messaging Interoperability and SMS/MMS Best Practices: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
- RFC 2045, Multipurpose Internet Mail Extensions Part One: https://www.rfc-editor.org/rfc/rfc2045
- RFC 5321, Simple Mail Transfer Protocol: https://www.rfc-editor.org/rfc/rfc5321
- OWASP, Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
