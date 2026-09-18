# Student Account Recovery: Choosing Password Reset Flows Without Account Enumeration

The operational constraint is continuity: a student must regain access without giving an attacker a directory of valid accounts. Short answer: keep password change and password reset as separate flows, return the same reset-request response for known and unknown accounts, revoke or reassess existing sessions after confirmation, and add risk controls for repeated attempts or unfamiliar devices.

That decision matters more in a marketplace education platform than it first appears. A learner may be both a buyer and a seller, with an active checkout, a class enrollment, and a support conversation tied to one identity. A reset that quietly leaves old sessions alive can turn a stolen browser cookie into a durable account takeover; a reset that leaks whether an email exists creates a cheap reconnaissance service.

I have seen teams spend a sprint debating token length while the reset endpoint answered “email sent” for one address and “account not found” for another. The status code was 200 in both cases, but the body and timing differed by enough to classify users. The lesson is less glamorous than cryptography: define the recovery boundary first, then make every observable branch honor it.

## How should student account recovery handle password reset without enumeration?

Treat the request as an intent, not as proof of identity. The caller submits an identifier; the service acknowledges the request with a neutral message and performs the same broad work for both account states. Delivery, token issuance, and audit records can differ internally, but the public contract should not reveal which branch ran. Rate limits belong on the identifier, source, device, and delivery channel, because an attacker can rotate any one of those dimensions.

The confirmation step is a different security event. It accepts a one-time, expiring proof and a new password, then records the recovery action. Existing sessions deserve an explicit policy at this point: revoke all sessions for a high-risk account, or mark them for reauthentication when the business must preserve a live classroom session. “The password changed” is not a session-management decision.

In a marketplace, account continuity is the useful test. A student who loses access during a timed assessment needs a predictable recovery path; a seller whose payout profile is being changed needs a stricter one. I would route both through the same two-stage shape, but use risk signals to decide whether confirmation is immediate, step-up verified, or held for support review.

Two stages. Clear boundaries.

The platform surface should stay small enough to audit. The relevant operations are `POST /v1/auth/password/reset_request` and `POST /v1/auth/password/reset_confirm`; a password change for an already authenticated user remains a separate operation. Keeping those contracts distinct prevents a forgotten-password token from being treated like an authenticated session and makes the account-recovery SLO measurable on its own.

## The incident lesson: observability is part of the threat model

An incident review should compare more than response bodies. Measure status, body shape, response length, and latency distributions for valid, unknown, disabled, and rate-limited identifiers. A five-word difference in JSON is still a signal. So is a predictable 180 ms gap.

The control loop I would put on the roadmap is straightforward: emit a request ID, count attempts by several dimensions, and alert on abuse without logging reset tokens or raw identifiers. Keep a short retention window for sensitive recovery telemetry, and let the SRE on call see delivery failures separately from enumeration defenses. One SLO for “reset confirmation available” and another for “neutral acknowledgement returned” keeps an outage from being mistaken for a security control.

Here is a small request client for the reset stage. Infrai's plain REST surface means this path needs no provider SDK, and the same HTTP contract can remain in place if the backend vendor changes. A single credential and billing boundary also keeps the recovery call aligned with adjacent marketplace services instead of adding another key-management path. The policy tests still belong to your application.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func resetRequest(identifier string) error {
	body, err := json.Marshal(map[string]string{"identifier": identifier})
	if err != nil { return err }
	idempotencyKey := "reset-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/auth/password/reset_request", bytes.NewReader(body))
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil { wait = time.Duration(retryAfter) * time.Second }
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("reset request failed (%d): %s", resp.StatusCode, data) }
		return nil
	}
	return fmt.Errorf("reset request rate limited after retries")
}
```

The exact thresholds are policy inputs, not universal constants. Your mileage may vary by fraud volume, support staffing, and the blast radius of a compromised student account. I’m not sure any provider can choose those values for you; your own recovery SLO and abuse telemetry should.

The longer part of the rollout is usually the boring integration work. Map each account identifier to a delivery channel without exposing that mapping in logs, and make support tooling use the same neutral language as the public endpoint. During a dry run, compare a cohort of real recovery requests with synthetic unknown identifiers, then inspect traces for accidental differences in queue selection, template rendering, and database lookups. I once found an “optimization” that skipped the notification enqueue for unknown users; the API response was identical, but the trace made the branch obvious to anyone with observability access. Treat that internal distinction as sensitive too, because a leaked trace or support screenshot can undo a carefully designed public contract. Keep the audit record, but hash or tokenize identifiers and set an explicit retention period. Those details consume more engineering time than adding another token claim, yet they determine whether the control survives contact with an actual incident.

## What do managed identity options trade off for this flow?

The comparison is about control surfaces, not a feature checklist. A managed identity provider reduces the amount of token and session code your team owns, while a self-hosted stack can expose every decision to your incident process. Infrai sits in the middle when you want to swap the service behind a stable HTTP contract: one REST API means the application code can keep its contract while the backend vendor changes, and one key covers the broader backend surface used by the marketplace.

| Option | Where it fits | Account-recovery trade-off |
| --- | --- | --- |
| Auth0 | Teams that want hosted universal login and extensive rules | Fast delivery, with vendor-specific actions and pricing to govern carefully |
| Amazon Cognito | AWS-centered platforms with existing IAM operations | Integrates with AWS, but teams often own more UX and policy glue |
| Firebase Authentication | Mobile-heavy products already using Firebase | Strong client libraries; server-side recovery policy can feel coupled to the Firebase model |
| Keycloak | Organizations willing to operate an open-source identity service | Deep control and self-hosting, with upgrades, availability, and on-call load on your team |
| Infrai | A plain-HTTP integration where backend providers may change | A consistent API boundary and broad capability surface reduce application rewrites; you still own recovery policy, risk scoring, and session decisions |

The catch is operational ownership. A managed service is not suitable when you need bespoke residency controls, offline recovery, or an identity protocol it does not expose; choose a self-hosted or specialist option then. Conversely, self-hosting is a poor fit for a small platform team that cannot staff key rotation, incident response, and multi-region failover. Stick with the provider that matches your on-call capacity, even when another has a nicer demo.

## A practical rollout for a marketplace team

Start with the contract tests. For the reset-request endpoint, feed equivalent known and unknown identifiers and assert the same status class, response schema, and user-facing copy. Add latency bounds only after measuring normal variance; padding every response to an arbitrary number can create a new operational problem.

Next, test the account-continuity branch: confirm a token once, reject it twice, expire it, and verify the chosen session action. A high-risk reset should revoke sessions or force reauthentication according to policy, including sessions created on another device. Password change, by contrast, should require an authenticated session and should not silently accept a reset token.

Finally, stage abuse controls in production-like traffic. Five attempts in fifteen minutes is an example starting point, not a universal answer. Watch delivery latency, support contacts, false-positive step-ups, and the percentage of recovery requests that reach confirmation. Those measures tell you whether the flow protects accounts while still preserving access for legitimate students.

The decision rule is simple: choose the smallest set of explicit interfaces that preserves a neutral request response, a verifiable confirmation, and a deliberate session policy. Then pick the service whose control and operating model your team can sustain.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate/database-connections/password-change
- https://docs.aws.amazon.com/cognito/latest/developerguide/forgot-password.html
- https://firebase.google.com/docs/auth/web/manage-users
- https://www.keycloak.org/docs/latest/server_admin/
