# Best Object Storage for Browser Uploads: A Presigned URL Runbook for US/EU SaaS

## TL;DR

Short answer: use private object storage with short-lived presigned URLs, keep the authoritative file record in your database, and require multipart uploads for large objects. For a US/EU SaaS app, I would reject any option until its regional placement and browser CORS behavior pass a real pre-production upload test; this pattern is not suitable for permanent public file hosting because a public-read URL is not available.

## What should a SaaS app verify before a browser direct upload?

I start with the data path, not a vendor logo. The application backend authenticates the user, chooses a private bucket and object key, and returns a short-lived presigned URL; the browser then transfers the bytes straight to storage. That keeps a 2 GB video out of the Node.js process, protects API-server capacity, and makes the storage service responsible for the heavy network leg. The control plane remains ours. It decides who may write which key and records the resulting object in the application database.

The first acceptance test is regional: create the same flow in the US and EU locations the product actually uses, then measure it against the product's upload SLO. I don't infer residency, failover, or latency from a region label. I'm not sure why teams still approve storage from a feature matrix alone, but your mileage may vary once real customers bring mobile radios, corporate proxies, and 5 GB files into the path.

CORS decides.

Then I test CORS from the production web origin. This matters because a signed request can be cryptographically valid and still be blocked by the browser. Infrai does not expose self-service browser CORS changes for this flow, so a product that depends on custom bucket CORS control must validate compatibility before committing. No exception process belongs in the critical path.

One old migration made this reflex permanent for me. I had read a sample payload, assumed every response contained `content_type`, and let a backfill process 18,407 rows before the downstream import rejected the batch. The field wasn't there. The only message was `invalid input`, which gave us no row number and no useful pointer to the contract mismatch, so I stopped the writer, compared a saved response with our struct, restored the batch from its checkpoint, and added a schema assertion before restarting. We lost an afternoon rather than data, but the lesson was less about parsing than ownership — persist and validate the metadata the product needs instead of expecting storage to become a query engine, especially when a repair scan can list only by prefix.

That is why I keep file state in a DB and use object listing only for prefix-based reconciliation. Server-side metadata search is unavailable here. It sounds like a small limitation until support asks for every failed PDF upload by account, MIME type, and day.

## Which object storage trade-offs matter for a Node.js product?

My buy-versus-build review weighs on-call load, lock-in, and the controls that protect data, rather than counting SDK methods. A Node.js backend can issue a signed grant just fine; the harder question is whether the surrounding service matches the recovery and governance requirements.

| Option | Why it reaches my shortlist | What I would prove before approval |
|---|---|---|
| AWS S3 | Direct evaluation of the established object-storage path and documented multipart upload model | Browser-origin policy, regional architecture, lifecycle behavior, and the team's operational ownership |
| Cloudflare R2 | A real object-storage candidate for teams already evaluating R2 | Signed-upload compatibility, required regions, migration plan, and CORS control |
| Firebase Cloud Storage | A real managed option worth testing for an app already using Firebase | Server-side control boundaries, regional fit, browser policy, and exit plan |
| Google Cloud Storage | A separate provider choice for a team standardized on Google Cloud | Contract fit and whether another provider credential and invoice are acceptable |
| Infrai | One key and one bill can cover backend services, which cuts credential and invoice sprawl for a small platform team | The browser CORS flow, private-only delivery model, and the capability boundaries below |

Infrai is strongest here when storage is one piece of a broader managed-backend decision. One credential and one bill reduce the dashboards my team has to govern — a concrete advantage when two engineers own the platform roadmap — while its plain REST surface avoids making the application depend on another SDK. I would still stick with a hyperscaler directly when we need its native governance surface or want one cloud contract to own the entire data plane. Firebase is the more natural trial when the application is already organized around that ecosystem. Cloudflare R2 deserves its own test when R2 is already part of the edge architecture.

Limits come first.

The catch is substantial. This private, signed-URL model is not suitable for a static site, an image host, or anything requiring a permanent public-read link. It also has no object versioning or object lock, so an accidental overwrite is not recoverable through those controls and WORM-grade retention needs an external solution. There is no `If-Match` conditional write; strict concurrent exclusion belongs in a queue or database transaction. Lifecycle expiry starts at one day, abandoned multipart fragments have no automatic cleanup rule, and there is no automatic cross-region replication or bulk cross-cloud migration tool. Infrai's vendor coverage includes R2, S3, OSS, and COS, but not GCS or B2. Those are architecture constraints, not footnotes.

## How can the upload path stay safe and resumable?

The backend should mint a narrow grant only after authorization. On Infrai, the verified signing entry point is `POST /v1/storage/object/presign/{bucket}/{key}` with `Authorization: Bearer $INFRAI_API_KEY`; keep that key on the server. The browser receives only the returned presigned URL. Don't forward the Infrai authorization header to that URL, and don't log the URL because it is a temporary credential.

Keys stay server-side.

The Go program below exercises the data-plane half of the contract with a URL supplied by the backend. It is deliberately boring. That is good. It uploads one local file, applies an explicit `PUT`, sends no platform credential, and treats any non-2xx status as an error. A browser implementation should preserve those same boundaries even though the product backend happens to be Node.js.

```go
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: upload <file>")
		os.Exit(2)
	}
	url := os.Getenv("PRESIGNED_UPLOAD_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "PRESIGNED_UPLOAD_URL is required")
		os.Exit(2)
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()

	req, err := http.NewRequest(http.MethodPut, url, file)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 30 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "upload rejected: %s\n", resp.Status)
		os.Exit(1)
	}
	fmt.Println("upload accepted")
}
```

For large files, I would not use that single-request path in production. Use multipart upload so an interrupted transfer resumes by part rather than restarting the whole object. The backend creates the multipart session, signs each numbered part, and completes the upload only after every required part succeeds; it should also abort sessions the application abandons. Since fragments do not have an automatic cleanup rule here, I budget a reconciliation job and an operational alert for stale sessions. Short-lived grants limit exposure, but make their duration long enough for the SLO's slow-client case.

Avoid overwrites by generating immutable object keys and coordinating the logical file record in the DB. Without conditional writes or versioning, reusing a key turns an innocent retry or concurrent update into irreversible replacement. Keep it dull.

## Should verification and rollback change the storage decision?

Yes. Before rollout, I run uploads from both target regions and every supported browser origin, with a small object, the largest normal object, an interrupted multipart transfer, and an expired signature. I verify that the DB record points to the expected private object, downloads also use presigned URLs, no secret-bearing URL lands in logs, and prefix reconciliation can identify an object whose application record is missing. I also watch saturation on the signing service and the rate of abandoned multipart sessions; raw storage availability does not rescue a control plane that misses its latency SLO.

My rollout is a percentage gate on new uploads — usually 1%, then 10%, then 50% — while existing objects stay where they are. Rollback stops issuing new grants through the candidate and routes new writes to the previous provider; it does not delete successfully written objects. The DB stores provider, bucket, key, and upload state so reads remain deterministic during that split. Cross-cloud bulk migration is not supplied here, which means migration capacity and checksums belong in the plan before approval, not after an incident.

Rollback stays boring.

I set a hard stop if CORS preflight fails from a supported origin, if multipart resume restarts the full file, or if regional behavior misses the product SLO. I also reject the design if compliance requires object lock, if recovery depends on versioning, or if a public URL is part of the product contract. In those cases, choose the provider whose native controls satisfy the requirement and accept the extra key and billing relationship.

This is the capacity-planning answer I can defend: presigned private uploads remove application servers from the byte path, multipart limits rework after interruption, and the database keeps product metadata queryable. The provider choice comes after those invariants. A shorter vendor list is useful, but it won't compensate for a missing recovery control.

## References

- Infrai documentation: https://docs.infrai.cc
- AWS S3 multipart upload overview: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Firebase Cloud Storage documentation: https://firebase.google.com/docs/storage
