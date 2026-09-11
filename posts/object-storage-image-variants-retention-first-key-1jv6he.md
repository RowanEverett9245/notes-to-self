# Object Storage Image Variants: Retention-First Keys for WebP, AVIF, and Backups

Short answer: the best file naming pattern for an edtech image pipeline is to make each original and derivative an immutable member of an upload generation in object storage, then let a database pointer decide what is published. Keep deletion as an explicit, auditable state transition. This is safer than making `original.webp` or `thumb-640.avif` a mutable address, especially when a tenant-scoped export can run beside a replacement or deletion request.

## The retention failure hides inside a successful upload

Consider a bounded workflow: a school tenant uploads a product image, a worker creates several sizes in WebP and AVIF, and an export job packages only that tenant's current images. The dangerous event is not necessarily a failed request. It is a successful write to a name that already meant something else.

If two uploads both derive `images/course-17/thumb-640.webp`, the later worker has silently changed the answer to an older export. A retry can do the same thing. A delete job that removes the shared prefix can erase a replacement that was published seconds earlier. The bucket accepted every operation; the application lost identity and retention semantics. In a real review of this workflow, I would trace the race as a timeline rather than inspect the final bucket state: upload A creates its original, upload B becomes current, a delayed resize for A writes the shared thumbnail name, and an export starts between those writes. Every individual request can return success, yet the export contains bytes that no longer correspond to the row it read. The useful evidence is the generation ID recorded with each manifest member, the database transition that made a generation current, and the deletion worker's list of keys. Without those three records, an incident turns into an argument about which filename was supposed to mean what.

Three words matter: identity, generation, role. An image ID identifies the logical record. A generation identifies one source upload and every derivative made from it. The role identifies `original`, a width, a format, or an archive copy. Put those parts into the key, and do not use a human filename as the identity.

The rule is boring. Good.

For a source image with ID `img_8f2c` and generation `upload-2026-08-11-a`, a key family could be:

```text
tenant/t_42/image/img_8f2c/upload-2026-08-11-a/original.jpg
tenant/t_42/image/img_8f2c/upload-2026-08-11-a/w320.webp
tenant/t_42/image/img_8f2c/upload-2026-08-11-a/w320.avif
tenant/t_42/image/img_8f2c/upload-2026-08-11-a/w1280.avif
tenant/t_42/image/img_8f2c/upload-2026-08-11-a/archive/original.jpg
```

The tenant prefix is useful for export scoping, but it is not an authorization check. Authorization still belongs in the application and in the export query. The exact separators are a local convention. The invariant is that two distinct source generations cannot calculate the same object key.

## How should object storage handle image thumbnails, multiple sizes, WebP, AVIF, originals, and backups?

Start with a manifest, not a bucket listing. The image row should record the current generation, source media type, creation time, retention deadline, and the set of expected derivatives. A derivative record can contain width, format, key, content hash, and status. The export worker reads that manifest under a tenant filter; it does not guess which objects are current from filenames.

That design gives each operation a bounded unit of work. A resize retry addresses one generation and one derivative. A reconciliation job can compare the expected manifest with addressed keys. Capacity planning can count the real expansion: five widths in two formats plus an original produces eleven objects per generation before an archive copy, and a new encoder configuration creates another generation rather than replacing the old bytes. The count is not a reason to panic; it is a reason to forecast storage, request volume, lifecycle work, and export time from the number of retained generations.

Publication should happen after the complete required set has been validated. Write the source and derivatives under a new generation, verify dimensions and media types, then update `current_generation` in one database transaction. Readers resolve the current pointer and construct or fetch only keys from that generation. A half-rendered set is therefore private work, not a partially public image.

Deletion needs the same discipline. Mark the logical image as `deletion_requested`, stop new derivative work, and capture the generation list in an audit record. A worker can then delete the addressed objects, record the result, and move the record to `deleted` only after the application has verified the required scope. If retention policy requires a backup, the backup must have its own retention rule and recovery objective; calling a second prefix a backup does not make it independent of the same failure domain.

Lifecycle expiry is housekeeping, not a transaction. It cannot decide which generation a user is allowed to see, and it should not be the only mechanism for a deletion request that has a contractual deadline. Keep the application state authoritative, and measure the delay between a request and verified completion as an SLO.

## The write path should make overwrites impossible by construction

The storage adapter should receive an already-qualified key. It should not be allowed to derive a public name from an original filename, and it should not decide which generation is current. Those decisions need the tenant, image row, retention policy, and publication transaction in view.

```go
package main

import (
	"fmt"
	"regexp"
	"strings"
)

var safePart = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func derivativeKey(tenantID, imageID, generation string, width int, format string) (string, error) {
	if !safePart.MatchString(tenantID) || !safePart.MatchString(imageID) || !safePart.MatchString(generation) {
		return "", fmt.Errorf("invalid identity component")
	}
	if width <= 0 {
		return "", fmt.Errorf("width must be positive")
	}
	format = strings.ToLower(format)
	if format != "webp" && format != "avif" {
		return "", fmt.Errorf("unsupported derivative format")
	}
	return fmt.Sprintf("tenant/%s/image/%s/%s/w%d.%s", tenantID, imageID, generation, width, format), nil
}

func main() {
	key, err := derivativeKey("t_42", "img_8f2c", "upload-2026-08-11-a", 640, "avif")
	if err != nil {
		panic(err)
	}
	fmt.Println(key)
}
```

The validation here is deliberately narrow. It rejects path separators and keeps the output format allowlist explicit; it does not claim that a key check replaces content validation. Upload controls still need server-side type checks, generated filenames, size limits, authorization, and storage outside the web root where appropriate. Those are the kinds of controls covered by the OWASP File Upload Cheat Sheet.

The export path needs a separate test. Given tenant `t_42`, it must select image rows owned by `t_42`, exclude rows in a deletion state, resolve each row's current generation, and copy only manifest members. Test the negative cases: a similarly named tenant, an old generation, a missing derivative, and a deletion request racing with export creation. A clean unit test is cheaper than finding cross-tenant bytes in a customer archive.

## Buy retention guarantees or build the control plane?

The meaningful buy-vs-build question is which property must be enforced by storage and which one the application can own. A naming pattern handles accidental replacement. It does not magically provide legal hold, independent copies, or a compare-and-swap transaction.

| Decision | What it gives the system | Cost or boundary to accept |
| --- | --- | --- |
| Application-managed immutable generations | Predictable retries, atomic publication through a database pointer, and targeted deletion | The team owns manifests, reconciliation, retention state, and on-call procedures |
| Storage versioning or immutability controls | A storage-enforced history or retention boundary for eligible objects | Policy configuration, restore procedures, and provider-specific behavior still need testing |
| Self-hosted object storage | Deployment and residency control | Hardware, upgrades, replication, monitoring, and recovery become platform work |
| Managed blob storage | Less storage operations work and a service-level boundary | The application still owns tenant authorization, export selection, and deletion semantics |

I would choose the first model for ordinary private thumbnails when the product can tolerate application-owned retention and the team can measure its deletion SLO. I would choose storage-enforced immutability for records that must survive an authorized delete request under a legal hold. The latter is a different requirement, not a more elaborate filename.

## Where this pattern is not suitable

The catch is that immutable names prevent one class of overwrite; they do not make a same-region archive a disaster-recovery copy. If the recovery objective requires another failure domain, use replication or a separately operated backup process and test restoration. If public, stable URLs are the product requirement, add a controlled delivery layer with a cache invalidation policy; a private object key alone is not that interface.

This pattern is also not suitable when editors need storage-level conditional mutation semantics that the application cannot serialize. Use a database compare-and-swap or a queue that orders updates, and make the losing generation harmless to readers. Do not use bucket listing as a lock.

I'm not sure one retention period can serve every tenant. A school may require a short deletion window for student-submitted material while an institution's exported records follow a longer contract. Resolve that uncertainty in policy configuration and audit data, then load-test deletion and export together. Your mileage may vary, but the measurement should be concrete: pending deletion age, current-generation lag, derivative completeness, export scope violations, restore time, and storage growth per retained generation.

The decision rule is straightforward: use stable logical IDs, fresh generation IDs, explicit derivative roles, and a database publication pointer; make tenant scope part of every selection query; and treat deletion as a workflow with evidence. Backups and overwrite recovery are separate capabilities. When retention is the product promise, the name is only the first control.

## References

- OWASP File Upload Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- Vercel Blob documentation: https://vercel.com/docs/vercel-blob
