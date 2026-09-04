# AI Image Custody: Downloads, Signed URLs, Retention, and Regional Backups

Choose the object store after you can prove the whole media lifecycle: an AI-generated image reaches its authorized user, an export expires on schedule, and a restore returns the right bytes in the right jurisdiction. Capacity and unit rates belong in the worksheet, but they cannot answer those questions.

Short answer: for AI-generated images, select storage by testing signed downloads, export behavior, retention and deletion, backups, and US/EU placement as separate, measurable paths.

The production lesson is easy to state and expensive to learn. A team can correctly lifecycle the original image prefix while letting asynchronously produced ZIP exports land under another prefix with no expiry rule. The store is behaving as configured; the design is still wrong. Those archives can accumulate, create a second access path, and distort both capacity forecasts and deletion commitments.

This is a data plane. Treat it like one.

## What should AI image storage prove for user downloads, exports, signed URLs, retention, backups, and US/EU placement?

Start by drawing the paths rather than the buckets. One service writes a generated image; another authorizes a user download; an export worker reads many source objects and writes a temporary archive; an operator may need to restore an object and its metadata. Each has different load, identity, failure, and audit properties. A storage comparison that collapses them into “supports objects” tells the on-call team almost nothing.

For each path, set an explicit objective. Useful measures include successful write latency, authorized-download success, export completion time, signed-link issuance success, deletion lag, recovery point objective, and recovery time objective. Keep issuing a signed URL distinct from retrieving an object. The first depends on the authorization and signing path; the second depends on the data path. Combining them can hide which dependency is spending the error budget.

The access rule should be boring: object keys are opaque, the application verifies tenant and user authorization before asking for a link, and a link is limited to the intended object and method for the shortest practical lifetime. Exports deserve their own contract: bounded manifest, size limit, output expiry, and recorded requester. A retry must not silently reread an unbounded set of source images.

US and EU are controls to verify, not friendly labels in a sales table. Map primary objects, replicas, backups, logs, and export artifacts to their actual locations, then test tenant routing against that map. FedRAMP is a US government-wide authorization program; it is not a synonym for choosing a US region. Legal and security owners need to translate residency, retention, and access obligations into tests that an engineering team can run.

## The export incident is a capacity-planning problem

Consider a bounded production scenario. The image service writes originals beneath a tenant-and-region prefix, then an export worker creates archives beneath an exports prefix. The originals carry a lifecycle policy; exports do not. Users retry large exports during a model launch, so read traffic and temporary archive bytes rise together. The first dashboard shows stored bytes. It misses the request fan-out, download delivery, and the fact that an archive can outlive its product purpose. A useful review walks one request all the way through: an authorized caller selects images, the service snapshots the IDs into a manifest, the worker reads each object, writes an archive, records its ownership and expiry, and returns a separate, short-lived access link. Then run the same trace for cancellation, a retry after a partial archive, deletion of one source image, a tenant move request, and a restore. The question isn't whether every individual API call succeeds. The question is whether the catalog, lifecycle policy, access path, and audit record still agree after those transitions. If they do not, don't launch a broad export feature and expect monthly storage graphs to reveal the discrepancy in time.

That sequence does not require a broken storage service. It follows from an incomplete policy boundary, and it is why I want the capacity model to have separate lines for source bytes, generated variants, write requests, reads per export, delivered bytes, replication or copy traffic, and temporary archives. Build two forecasts: a normal week and a retry-heavy launch week. The latter is the operational forecast because queues, client backoff, and lifecycle processing all see the burst instead of the monthly average.

The preventive control belongs before the write. The application should accept placement and retention as deliberate inputs, attach integrity metadata, record the object in its catalog, and only issue a short-lived download after authorization. The provider-specific signing code stays behind a tested adapter, which makes an exit exercise possible without pretending that object APIs have identical policy semantics. Don't let a caller select a prefix and call that policy.

```go
package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
)

type Placement string

const (
	PlacementUS Placement = "us"
	PlacementEU Placement = "eu"
)

type PutOptions struct {
	Placement   Placement
	ContentType string
	SHA256      string
	RetainUntil time.Time
}

type ObjectStore interface {
	Put(context.Context, string, io.Reader, PutOptions) error
	SignDownload(context.Context, string, time.Duration) (string, error)
}

type Catalog interface {
	Record(context.Context, string, string, Placement, string) error
	CanDownload(context.Context, string, string, string) (bool, error)
}

type Service struct {
	store   ObjectStore
	catalog Catalog
	now     func() time.Time
}

func (s *Service) Publish(ctx context.Context, tenantID, key, contentType string, placement Placement, retainUntil time.Time, image []byte) error {
	if tenantID == "" || key == "" || len(image) == 0 {
		return errors.New("missing required media attributes")
	}
	if placement != PlacementUS && placement != PlacementEU {
		return fmt.Errorf("unsupported placement %q", placement)
	}
	if !retainUntil.After(s.now()) {
		return errors.New("retention must end in the future")
	}

	digest := sha256.Sum256(image)
	checksum := hex.EncodeToString(digest[:])
	if err := s.store.Put(ctx, key, bytes.NewReader(image), PutOptions{
		Placement: placement, ContentType: contentType, SHA256: checksum, RetainUntil: retainUntil,
	}); err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return s.catalog.Record(ctx, tenantID, key, placement, checksum)
}

func (s *Service) DownloadURL(ctx context.Context, tenantID, userID, key string) (string, error) {
	allowed, err := s.catalog.CanDownload(ctx, tenantID, userID, key)
	if err != nil {
		return "", fmt.Errorf("check download access: %w", err)
	}
	if !allowed {
		return "", errors.New("download denied")
	}
	return s.store.SignDownload(ctx, key, 10*time.Minute)
}
```

The code is intentionally incomplete as an adapter implementation: policy translation and signing algorithms are service-specific. The reviewable invariant is not. Tests should reject an invalid placement or expired retention, preserve the checksum, deny cross-tenant download, constrain link lifetime, and verify that EU-policy objects do not surface in a US inventory. Run those tests during deployment and as scheduled probes, since a policy can drift long after the application code stays unchanged.

## Retention, deletion, and recovery are different promises

Retention answers how long an object must or may exist. Deletion answers who can remove it and how that intent propagates. Backup answers whether a separate recovery path can survive operator error, corruption, or a bad lifecycle rule. Replication may improve availability, yet a valid deletion can propagate to the replica; it is not automatically a backup.

Give originals, derivatives, and exports separate lifecycle classes because their value and exposure differ. Record the class and immutable tenant placement beside the object, keep backup credentials separate from live-data credentials, and write deletion as an observable workflow: record the request, stop issuing new signed links, remove eligible data, then retain an auditable result. For holds or regulated retention, use an approved control path rather than a hurried prefix-policy change.

Recovery needs an SLO of its own. Define acceptable data loss and restore time, choose an isolation and backup interval that can satisfy them, then restore into a clean environment. Verify object bytes, checksums, metadata, placement, and authorization records. A completed backup copy proves that data moved; only a restore drill proves that the service can return it before the recovery objective runs out.

## Buy versus build: where does the pager land?

Object compatibility says little about the work behind it. Managed storage generally transfers fleet operation to a provider, while the application team still owns identities, data mapping, lifecycle policy, export design, and recovery validation. Self-hosting can offer tighter operational control, but it puts upgrades, capacity procurement, telemetry, and incident response directly on the platform roadmap.

| Approach | Best fit | Team still owns | Exit test |
|---|---|---|---|
| Managed object service | Variable demand and a small storage operations team | IAM, lifecycle policy, data map, restore validation | Export objects and metadata, then replay checksum and signed-download tests through another adapter |
| Self-hosted object API | Existing storage operators and constrained infrastructure choices | Hardware or capacity, upgrades, durability operations, and the pager | Rebuild a clean cluster and restore a representative tenant under the recovery objective |
| Split design | Different residency or archival requirements by data class | Routing rules, observability, and data-movement controls | Prove every class has one authoritative catalog record and a tested recovery path |

The catch is organizational, not syntactic. Self-hosting is not suitable when nobody can own storage operations around the clock; a managed service is a poor fit when its placement, retention, audit, or export semantics cannot meet the written control set. Stick with the option that passes restore, routing, and access tests under expected peak load, then rehearse the exit path while the dataset is still small enough to move. You can't outsource the decision about acceptable data loss.

## References

- https://developers.cloudflare.com/r2/
- https://www.fedramp.gov/
