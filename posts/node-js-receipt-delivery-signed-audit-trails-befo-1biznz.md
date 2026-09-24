# Node.js Receipt Delivery — Signed Audit Trails Before Confirmation Email

An order-confirmation page fires because the receipt-delivery age has crossed its service-level objective: the order exists, payment is complete, but the customer still has no email. The on-call view should show an order pseudonym, the current job stage, attempt count, document digest, signing identity, and next retry time. It should not show the customer's address, name, or raw receipt.

**TL;DR:** the least complex dependable design is an Express checkout service that commits an outbox record with the order, followed by an idempotent background worker that renders the PDF, redacts fields according to the sharing policy, validates the result, signs a manifest containing the final document hash, stores an append-only audit event, and only then submits the email. A page on end-to-end delivery age catches stuck work that queue-depth alerts miss. The signature proves which exact bytes were approved; the audit trail explains who or what approved them and why.

This ordering matters. Signing a pre-redaction file authenticates the wrong artifact, while logging personal data to make incidents easier to debug creates another copy that must be protected and eventually deleted. The durable unit is not “send an email.” It is a state machine whose evidence survives a process crash.

## What should the page tell the on-call engineer?

Start at the customer-visible symptom and walk backward. An alert such as “99% of paid orders receive a submitted confirmation within 10 minutes over a rolling 30-minute window” is a proposed SLO, not a universal target; choose the duration from business tolerance and observed provider latency. The page needs separate counts for work waiting, actively leased, retryable, quarantined, and completed. One large `failed` bucket hides the difference between a transient email rejection, an invalid PDF, and a privacy-policy violation.

The first triage question is whether the oldest eligible job is moving. Queue depth alone is weak evidence: a flash sale can produce a large but healthy backlog, and a single poisoned job can remain old while the total depth looks ordinary. Track age at every boundary instead: checkout commit to job claim, claim to validated artifact, validation to signature, and signature to email submission. A stage-age histogram and an oldest-item gauge make the stalled boundary visible.

Depth can lie.

Keep identifiers deliberately boring. Use an internal receipt ID in metrics, and place the order ID only in restricted structured logs when cardinality and access controls permit it. Email addresses, customer names, postal addresses, and unredacted line-item notes do not belong in metric labels. The alert should link to a runbook query keyed by the pseudonymous job ID, with authorization around any lookup that can reconnect it to a customer.

The page is actionable when it distinguishes four responses: increase worker capacity for a growing eligible backlog; pause delivery when redaction validation fails; rotate or repair signing access when signatures cannot be produced; and allow bounded retries for transient delivery failures. Those are different failure domains. Treating them as one retry loop can repeatedly distribute a document that should have been quarantined.

## How should Node.js attach a receipt PDF to an order confirmation?

The checkout request should not render a PDF or call an email service. In the same database transaction that records the paid order, it records an outbox event with a stable job ID and the immutable input revision. A relay publishes that event, and duplicate publication is expected. The worker therefore claims work with a lease and uses the job ID plus input revision as its idempotency key.

Rendering produces a candidate, not a deliverable. The next step applies the explicit sharing policy: retain receipt fields needed by the recipient and remove personal data outside that purpose. Then validate the resulting PDF and compute a cryptographic digest over the final bytes. ISO 32000-2 defines PDF 2.0; conformance to a declared PDF format is a better boundary than trusting a renderer's successful return value.

Sign a small manifest rather than an operational log line. The manifest can contain the receipt ID, order revision, policy revision, renderer revision, final SHA-256 digest, generation time, and signing-key identifier. Its signature binds the decision context to the exact attachment without putting customer data into the audit stream. Record the manifest, signature, and state transition in append-only storage before email submission.

The email step needs its own stable submission key and an explicit uncertainty state. A timeout after submission does not prove that nothing was accepted. Blindly submitting again may send two confirmations. If the delivery boundary cannot answer an idempotent lookup, move the job to reconciliation after an ambiguous result instead of pretending every network error is safely retryable.

Here is a compact worker core in Go. It leaves rendering, policy enforcement, signing, persistence, and delivery behind interfaces so the control flow can be tested without coupling the receipt pipeline to one queue or mail product.

```go
package receipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Job struct {
	ID, ReceiptID, OrderRevision, PolicyRevision string
}

type Manifest struct {
	JobID, ReceiptID, OrderRevision, PolicyRevision string
	DocumentSHA256, GeneratedAt, KeyID              string
}

type Services interface {
	Render(context.Context, Job) ([]byte, error)
	Redact(context.Context, []byte, string) ([]byte, error)
	ValidatePDF(context.Context, []byte) error
	SignManifest(context.Context, Manifest) (signature []byte, keyID string, err error)
	CommitReady(context.Context, Manifest, []byte) error
	SubmitEmail(context.Context, string, []byte) error
}

var ErrQuarantine = errors.New("receipt requires quarantine")

func Process(ctx context.Context, svc Services, job Job, now time.Time) error {
	candidate, err := svc.Render(ctx, job)
	if err != nil {
		return err
	}
	finalPDF, err := svc.Redact(ctx, candidate, job.PolicyRevision)
	if err != nil {
		return ErrQuarantine
	}
	if err := svc.ValidatePDF(ctx, finalPDF); err != nil {
		return ErrQuarantine
	}

	digest := sha256.Sum256(finalPDF)
	manifest := Manifest{
		JobID: job.ID, ReceiptID: job.ReceiptID,
		OrderRevision: job.OrderRevision, PolicyRevision: job.PolicyRevision,
		DocumentSHA256: hex.EncodeToString(digest[:]), GeneratedAt: now.UTC(),
	}
	signature, keyID, err := svc.SignManifest(ctx, manifest)
	if err != nil {
		return err
	}
	manifest.KeyID = keyID
	if err := svc.CommitReady(ctx, manifest, signature); err != nil {
		return err
	}
	return svc.SubmitEmail(ctx, job.ID, finalPDF)
}
```

There is an intentional sharp edge in this example: `CommitReady` must atomically preserve the signed evidence and the transition to “ready for delivery.” If those are two independent writes, a crash can leave a ready job without its audit proof. The reverse ordering is no better: evidence recorded before the state transition may describe a release that never became eligible. Put both records in one transactional boundary when they share a database; if they cannot, use a durable intent record and a reconciler that can prove which side completed. Also, production code should not keep a large PDF resident through every adapter if receipts can be large; stream through bounded storage while hashing, then pass an immutable object reference to the delivery adapter. The stored reference must be immutable or version-addressed, because replacing bytes at the same key after signing silently breaks the relationship among the digest, attachment, and audit entry. Finally, treat a policy-revision mismatch as quarantine rather than a generic retry: repeated execution cannot make an obsolete redaction decision correct.

No shortcuts here.

## The signal that should have fired first

The page described above is late by definition. Earlier warning comes from the rate at which jobs stop advancing between stages. Instrument each state transition with a monotonic duration, outcome, and low-cardinality reason code. Useful reason classes include `render_invalid`, `redaction_rejected`, `sign_unavailable`, `delivery_transient`, and `delivery_ambiguous`; raw exception strings belong in controlled logs, not metric labels.

Measure both throughput and saturation. Worker CPU can look quiet while every task waits on signing or object storage, so capture lease utilization, external-call latency, retry scheduling delay, and the age of the oldest eligible item. Compare arrival rate with sustainable completion rate over a window long enough to ignore brief bursts. Capacity planning then becomes concrete: if sustained arrivals exceed verified completions, the backlog must grow, regardless of how reassuring the average latency appears.

The instrumentation change I would make first is a transition ledger emitted from the same state-machine boundary that commits progress, rather than scattered timers around library calls. Each event carries job ID, from-state, to-state, reason code, attempt, occurred-at time, and trace ID. Dashboards derive stage latency from those events; an audit store retains the signed manifest separately. Operational telemetry and compliance evidence can share correlation identifiers without sharing retention periods or access rules.

Test the alert with fault injection before trusting it. Hold signing responses, make validation reject a well-formed but policy-invalid fixture, delay the relay, and force an ambiguous delivery timeout. The expected result is more specific than “the alarm rings”: the correct stage-age signal should breach, unsafe work should stop, eligible work should continue where isolation allows it, and the runbook should identify the decision owner.

## Signature, audit, and operating choices

A PDF signature and an audit trail answer related but different questions. The signature can establish integrity and associate approved bytes with a key. The audit trail records the surrounding transition: policy revision, actor or workload identity, time, reason, and prior state. Neither proves that a redaction rule was appropriate. That requires policy review and test fixtures representing the e-commerce data that may appear on a receipt.

Stop on doubt.

Key custody is the primary buy-versus-build axis because it changes both incident impact and on-call work. The table is deliberately about operating models, not brands.

| Operating model | Signature boundary | Audit consequence | SRE trade-off |
| --- | --- | --- | --- |
| Application-managed keys | Worker process or a library it calls | Team must prove key access, rotation, and event integrity | Maximum control, largest security and on-call surface |
| Self-hosted signing service | Internal network service | Central evidence format and policy, plus a service to patch and scale | Reduces key exposure in workers but adds a critical dependency |
| Managed signing service | External service boundary | Provider records must be correlated with the internal state ledger | Less key infrastructure; dependency SLO, egress, and lock-in require review |

Choose by failure ownership. A managed boundary may reduce key-handling work, but it does not own the correctness of the redaction policy or the decision to release a receipt. A self-hosted service can keep custody under one control plane, but its availability becomes part of confirmation delivery. Application-managed keys are defensible only when the team can sustain rotation, access review, backup, and incident response without turning the checkout worker into a security subsystem.

Retention deserves the same precision. Keep the minimum evidence needed to verify the artifact and reconstruct the state transition; store the personal document under its own access and deletion policy. A digest in an audit record is useful only while investigators can establish what object it referred to and which key signed the manifest. Document deletion, key retirement, and audit retention therefore need an explicit relationship rather than three unrelated timers.

## Thresholds spend an error budget too

An alert threshold is a capacity decision disguised as monitoring configuration. Set the page too close to normal burst behavior and on-call engineers will learn to distrust it; set it beyond the customer promise and the alert merely confirms damage. Start with the delivery SLO, subtract realistic diagnosis and recovery time, and place the page where intervention can still protect the objective. Use a lower-severity ticket for slow capacity drift and a page for fast budget consumption or a safety stop such as repeated redaction rejection.

Evaluate the threshold against historical arrival shapes without claiming that yesterday's maximum is tomorrow's limit. Record how many pages would have fired, how long they would have remained actionable, and whether each one maps to a response in the runbook. A threshold that produces frequent alerts during healthy sale bursts has a real cost: interrupted engineers, rushed overrides, and a higher chance that a later privacy failure is ignored.

The final decision rule is strict: do not send unless the final, redacted PDF validates; its digest is bound into a signed manifest; the audit transition is durable; and delivery can be retried or reconciled without guessing. Everything else, including queue technology and renderer choice, is replaceable implementation detail.

## Further reading

- ISO, “ISO 32000-2 — Portable Document Format”: https://www.iso.org/standard/75839.html
