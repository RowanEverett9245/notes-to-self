# Retention-First Direct Browser Storage Uploads: Presigned URLs, CORS, Node.js SaaS

Short answer: for an e-commerce SaaS accepting private user uploads, use a backend-issued, short-lived presigned URL for the browser transfer, but choose the storage service by its retention and deletion controls, not by the headline price per gigabyte. The application should own the object key, expiry, checksum, and lifecycle state; storage should never be the only record of what must be deleted.

That decision sounds narrow until a customer asks to remove an order's receipt, a download link survives a subscription cancellation, or an upload is abandoned halfway through a checkout flow. The bytes are easy. The state transitions are the work.

## The retention cost starts with deletion

Start with the deletion contract. Define how long an object may exist, what event starts the retention clock, what happens when deletion is retried, and how the system proves that a deletion request was applied. A useful record has a tenant ID, an application-owned object key, the storage location, a content checksum, an upload state, a retention deadline, and timestamps for verification and deletion. It should not store a permanent public URL as the canonical reference.

The upload state machine can stay small:

`issued -> uploading -> verified -> retained -> deletion_requested -> deleted`

An expired presign is not a deletion mechanism. It only limits future access. A verified object still needs an explicit deletion path, and a failed deletion needs a retry policy with an audit trail rather than a quiet status change in the database.

The SLO should measure the customer path, not just the storage API. Track the proportion of accepted uploads that become verified objects within the client timeout, then track deletion completion within the promised window. Break those measurements into presign issuance, browser transfer, verification, signed download, and deletion. This makes a CORS rejection visible instead of allowing a healthy backend metric to hide a broken browser workflow.

Keep one sentence in the runbook: delete means delete from the application index and request deletion from storage, then reconcile until the object is confirmed absent. That is the operational boundary.

For example, an order-support agent may upload a return label, replace it twice, and then close the case while a customer still holds the first download link. The object records cannot be inferred from the current order row after those replacements, especially if a retry created a second application attempt before the first response arrived. The ledger therefore needs one immutable object key per upload, a deletion deadline attached to each key, and a reconciliation job that can distinguish “requested,” “confirmed absent,” and “not found during inspection.” A missing object is usually the desired end state, but it is not proof that the corresponding database row was retired, and a retired row is not proof that every provider-side version or incomplete multipart session has been handled. That distinction is where retention promises become testable engineering work rather than a checkbox on a storage dashboard.

## How should a Node.js SaaS make direct browser storage uploads with presigned URLs?

The trusted backend authenticates the user, chooses a tenant-scoped key, and issues a short-lived signed upload instruction. The browser uses that instruction to send the bytes directly to storage. Afterward, the backend verifies the object and only then marks the application record ready; download uses a separate short-lived signed instruction.

This removes storage credentials from frontend code and keeps the application server out of the byte path, but it does not remove browser policy. CORS must allow the exact production origins, method, and request headers used by the signed request. A preflight can fail before storage sees an upload, so test it from an actual browser on every approved US and EU origin. MDN's CORS guide is the right baseline for understanding which request the browser sends and which response headers it requires.

The backend boundary can be expressed with a provider-neutral interface. The implementation behind it may call an S3-compatible API or another storage API, but the rest of the application should depend on states and intents rather than provider-specific URLs.

```go
package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
)

type ObjectStore interface {
	IssuePut(ctx context.Context, key string, expires time.Duration) (string, error)
	IssueGet(ctx context.Context, key string, expires time.Duration) (string, error)
	Delete(ctx context.Context, key string) error
	Head(ctx context.Context, key string) (size int64, checksum string, err error)
}

type Upload struct {
	TenantID       string
	Key            string
	ExpectedBytes  int64
	ExpectedDigest string
	State          string
	DeleteAfter    time.Time
}

func Begin(ctx context.Context, store ObjectStore, tenantID, key string, size int64) (Upload, string, error) {
	if tenantID == "" || key == "" || size <= 0 {
		return Upload{}, "", errors.New("tenant, key, and positive size are required")
	}
	putURL, err := store.IssuePut(ctx, key, 15*time.Minute)
	if err != nil {
		return Upload{}, "", fmt.Errorf("issue upload instruction: %w", err)
	}
	return Upload{
		TenantID:      tenantID,
		Key:           key,
		ExpectedBytes: size,
		State:         "issued",
		DeleteAfter:   time.Now().UTC().Add(30 * 24 * time.Hour),
	}, putURL, nil
}

func Verify(ctx context.Context, store ObjectStore, upload Upload) (Upload, error) {
	size, digest, err := store.Head(ctx, upload.Key)
	if err != nil {
		return upload, fmt.Errorf("inspect uploaded object: %w", err)
	}
	if size != upload.ExpectedBytes {
		return upload, fmt.Errorf("size mismatch: got %d, want %d", size, upload.ExpectedBytes)
	}
	upload.ExpectedDigest = digest
	upload.State = "verified"
	return upload, nil
}

func Digest(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
```

The example deliberately does not pretend that a signed URL is a policy engine. Size, content type, checksum, and tenant authorization still need validation at the application boundary, and the storage adapter must map its provider's verification and deletion semantics into the state machine. Do not log the signed URL. Do not accept a client-selected path without constraining it to the tenant's namespace.

## Compare the whole transaction, not the byte rate

“Cheapest” is a workload calculation, not a durable property of a vendor label. For each candidate, measure one complete transaction: presign, browser upload, verification, expected downloads, deletion, abandoned upload cleanup, cross-region traffic, and the staff time required to operate it. Use the same object-size distribution and US/EU traffic split. A price cell that excludes downloads or incomplete multipart parts is not a comparison; it is a partial invoice.

| Choice pattern | Keep it in the evaluation when | Proof required before launch |
|---|---|---|
| S3-compatible object store | The team wants a familiar object API and can validate compatibility | Browser preflight, signed upload/download, multipart completion, abort, lifecycle, and deletion tests |
| Large hyperscale object store | Regional controls, mature operational tooling, or a broad ecosystem matter | The same transaction test plus region, egress, retention, and support-cost review |
| Application-platform storage | The team values a close authorization boundary and a smaller integration surface | Export, deletion evidence, limits, CORS, and exit-path tests |
| Upload-focused service | The product needs an opinionated upload workflow and accepts its integration boundary | Retry, cancellation, retention, download, quotas, and migration tests |
| Self-hosted object storage | The organization can carry capacity, durability, patching, and on-call ownership | Failure-injection, replication, restore, upgrade, and deletion-verification tests |

The buy-versus-build decision belongs in the same review. Managed storage usually trades infrastructure toil for recurring provider dependency; self-hosting trades invoice visibility for capacity planning, hardware or compute ownership, repairs, upgrades, and a larger SLO surface. An S3-compatible API can reduce application changes during a move, but API compatibility is not the same as identical lifecycle, replication, conditional-write, or deletion guarantees.

I would not sign off on a cheapest-option claim without a month of representative traces, and I'm not sure any static comparison survives a major shift from uploads to downloads. Your mileage may vary. The useful output is a cost envelope with assumptions, not a winner's badge.

## A release canary proves the contract

The release canary should run from a real browser, not only from a Node.js test client. On every production origin, it should issue a signed upload, send a private object, verify its checksum, issue a signed download, compare the bytes, request deletion, and confirm that a later read is denied or absent according to the storage contract. Exercise an expired URL too. It should fail cleanly and leave an application record that reconciliation can explain.

Large objects need a separate multipart scenario. Multipart upload has distinct creation, part upload, completion, and abort operations; AWS documents this lifecycle in its multipart overview. Persist the upload identity and received parts, complete only after the expected parts are present, and explicitly abort cancellation and timeout paths. A daily lifecycle rule may be useful as a backstop, but it is not a substitute for prompt application-owned cleanup.

The rollback boundary should be the storage adapter. Persist the provider reference and object key with each record so new uploads can be paused or redirected while existing private objects continue to use the location that owns them. A migration requires copy, checksum verification, rate limiting, a dual-read period if necessary, and a reversible cutover. Keep it boring.

Alert on expired presign spikes, CORS preflight rejection, verification lag, checksum mismatch, deletion backlog, and abandoned multipart count. Stop issuing new upload instructions when the end-to-end upload or deletion SLO burns its error budget. Allow already-issued instructions to expire, reconcile their records, and resume only after the browser canary passes in both regions.

The recommendation is intentionally conditional: choose a different storage boundary when you need legal hold, strict object versioning, provider-native replication, permanent public assets, or deletion semantics your application cannot verify. Stick with the service that exposes the control required by the correctness model, even when its first-page price looks less attractive. Retention is a product promise; it deserves more than a bucket setting.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
