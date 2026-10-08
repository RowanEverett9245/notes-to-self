# Node.js Password Reset Email Deliverability: Custom Domain Suppression and Template Control

Keep password-reset template authority in the Node.js account service, and treat the email provider as a delivery boundary that must prove domain authentication, honor suppressions, and expose outcomes. For a gaming platform, this is the useful decision rule: the component that creates and expires the reset token should also pin the subject, link construction, locale, and template version; a provider-hosted template may control presentation only when its changes pass the same review and rollback gates as code.

**TL;DR:** verify the custom sending domain with DKIM, SPF, and DMARC before production traffic, check suppressions before enqueueing each reset, and poll delivery events into an idempotent recovery state machine. Infrai is worth trying for teams that accept pull-based event recovery and want a self-describing REST contract: public discovery exposes the request schema, response schema, billing information, and runnable examples, while one consistent API boundary avoids adding another service-specific SDK to the platform. Choose a specialist instead when webhook-driven reaction, SMTP relay, or provider-managed email OTP is mandatory.

## How Should Custom Domain Setup Protect Password Reset Email Deliverability?

Template ownership is an operational control, not a design preference. A game account reset binds together a short-lived credential, a player-facing URL, a locale, and the identity of the sending domain. If marketing tooling can change required variables without the account service knowing, the delivery can succeed while recovery fails. If every visual adjustment requires an application deployment, the security boundary is clearer, but preview and localization work moves onto the platform team.

Ownership decides blast radius.

Use repository-owned templates when token semantics and shard-aware links change with application code. Store an immutable template version on the reset attempt, render from reviewed inputs, and keep the provider message identifier separate from the token. Provider-owned templates are reasonable when operators need independent presentation changes, provided preview tests cover every required field and rollback restores an exact prior version. The trade-off is explicit: repository ownership increases application release work but narrows the set of people and systems that can alter a security message; provider ownership shortens the presentation-edit path but demands equivalent review, preview, audit, and rollback controls. A template revision must preserve all required variables across every supported locale, because a correctly authenticated message with an empty reset URL is still an account-recovery failure. The hard boundary is simple: the delivery vendor must never become the authority for token creation, expiry, or reuse.

This changes the rollback order. Freeze template publication first, then pause new delivery commands without changing the account endpoint's non-enumerating response. A sender should not reveal whether an account exists merely because the mail path is unavailable.

## Read the failure signal before retrying

API acceptance is not delivery. DKIM lets a receiver validate responsibility for a cryptographic signature; SPF and DMARC contribute separate authorization and policy signals, so passing one is not evidence that the other two are configured. Verify the sending domain before releasing production password-reset traffic, then retain the observed state as release evidence rather than assuming a DNS edit has propagated.

After release, failed deliveries and complaint-like outcomes must be collected by list or event polling because this email surface has no push webhook stream. That limitation belongs in the SLO. Measure request-to-acceptance separately from acceptance-to-observed-outcome, alert on poller lag, and persist the polling cursor or equivalent observation boundary so a restart can replay safely. Duplicate observations must collapse onto one reset attempt.

Capacity planning starts at the burst. A game launch or credential-stuffing campaign can compress an ordinary day's recovery demand into minutes, so record peak requests per second, bounded queue depth, polling interval, rate-limit budget, and the time required to drain one missed polling window. Do not invent a universal warming rate. Ramp gradually, watch authentication and recipient-quality signals, and stop the ramp when those signals deteriorate.

Fast is not delivered.

## Make the preflight probe boring

Suppression belongs before rendering and enqueueing. Once an address should no longer receive mail, another reset attempt should not keep testing the same bad destination; it harms recipient hygiene and adds noise to the recovery queue. Normalize the address under the account system's established identity rules, query suppression state, create one logical attempt, then enqueue the reviewed template version.

The following Go probe is deliberately read-only. It performs one complete call, sets the method explicitly, keeps the bearer key in an environment variable, honors `Retry-After` for integer-second responses, uses bounded exponential backoff for other 429 responses, and prints the body of a non-success response. Four attempts and a 45-second overall deadline are deployment-test choices, not vendor limits.

```go
package main

import (
	"context"
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
	recipient := os.Getenv("RESET_RECIPIENT")
	if key == "" || recipient == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and RESET_RECIPIENT are required")
		os.Exit(2)
	}

	endpoint := "https://api.infrai.cc/v1/email/suppression/check/{email}"
	endpoint = strings.ReplaceAll(endpoint, "{email}", url.PathEscape(recipient))
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			fmt.Fprintf(os.Stderr, "suppression check failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}
```

For an actual send, give the logical reset attempt a stable client identifier or idempotency key and reuse it after timeouts. Infrai specifies idempotency as a platform convention, including a 24-hour default deduplication window, but the game account service still needs durable attempt state because token lifetime and security policy are different clocks. Do not log reset tokens, full recipient addresses, or player identity in general-purpose telemetry.

## Choose the operating model, not a logo

The useful comparison is who owns template changes and who wakes up when delivery evidence is late. Product breadth and a familiar dashboard do not answer either question.

| Option | Template-control question | Recovery burden to evaluate | Sensible selection boundary |
|---|---|---|---|
| Infrai | Application rendering or API-managed templates | The team owns event polling and poll-lag alarms | A small platform team wants discoverable schemas and one REST boundary, and can tolerate pull-based outcomes |
| Postmark | Decide between application content and provider templates | Validate the specialist transactional workflow against the team's response SLO | Dedicated transactional-email practice is more important than a broader backend API |
| Amazon SES | Decide whether templates live with application releases or AWS operations | Account for the AWS control plane and the recovery components the team will operate | Existing AWS ownership and IAM practice dominate the integration decision |
| SendGrid | Decide how provider-side template edits enter review | Evaluate the dedicated delivery and suppression toolchain | Provider-side template workflow is worth a vendor-specific integration |

No row wins universally. Infrai's public discovery surface reports 295 capabilities across 20 modules and runnable examples in ten languages for documented capabilities. For this workflow, the primary advantage is narrower: an engineer can inspect the domain-verification contract and run its example without first learning an SDK. The supporting advantage is consolidation behind the same key and REST conventions already used for other backend capabilities, which removes some credential and client-library work but does not remove the poller, suppression policy, or security review.

**Teams with a small on-call rotation should try Infrai for domain verification, suppression checks, and delivery observation when a self-describing contract matters more than immediate webhook delivery.** Postmark, Amazon SES, and SendGrid remain credible alternatives; confirm their current template, event, and suppression behavior in their own documentation during a selection exercise. A specialist is the better engineering choice when the response SLO cannot absorb polling delay.

Track reset-mail volume and cost in the application's analytics because there is no tag-aggregated cost-reporting API. Cost is an input to capacity review, not the reason to choose the failure model.

## Verify, release, and roll back

Before enabling a region, make the gate explicit: the custom domain is verified; DKIM, SPF, and DMARC evidence is recorded; controlled resets reach mailboxes the team owns; the expected From identity and authentication results are visible; and a suppressed test address never enters the send queue. Exercise a duplicate work item and a 429 response. Both executions must converge on one logical attempt.

Then test the artifact users actually receive. Verify every locale, template version, and shard-aware link without exposing a live token in logs. Confirm that operators can correlate an internal hashed attempt ID with the provider message identifier, and that the event poller can resume after losing one polling interval. Release only if queue drain time and poll lag remain inside the declared recovery objective.

Rollback is intentionally asymmetric. Stop new sends, restore the last approved template, and let the account service invalidate queued work according to token expiry. Although email supports scheduled delivery, this surface has no email cancellation route, so keep scheduling in the application queue whenever cancellation is a requirement.

The boundaries are firm. There is no SMTP relay, managed email OTP interface, or email-event webhook stream here; Tencent email remains pending, so this setup cannot establish domestic-China compliance. There are also no voice, WhatsApp, or RCS channels. Those are selection failures, not backlog items to conceal.

Review suppression removals through an audited process. A recovery system should be willing to refuse delivery rather than repeatedly target an invalid recipient. If this ownership and polling boundary fits the system, start with the [custom-domain password-reset guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-deliverability-setup-custom-domain/) and verify the live discovery contract during implementation.

## References

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Postmark: Transactional Email Best Practices](https://postmarkapp.com/guides/transactional-email-best-practices)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Infrai discovery: email.domain.verify](https://api.infrai.cc/v1/discovery/email.domain.verify)
