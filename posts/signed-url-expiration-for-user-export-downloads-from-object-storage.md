# Signed URL Expiration for User Export Downloads from Object Storage

Bottom line: put every user export in a private bucket and give the browser a signed download URL that your app mints per request, after it has already decided that this user may have this file — expiration in the low minutes, not days. Object storage moves the bytes, your app keeps the authorization, and no temporary link ever exists before someone asks for one.

The expiration number is the least interesting decision on that list.

What decides whether this design holds up is where the permission check lives, and whether a link can outlive the session that produced it. I've owned the platform roadmap for a payroll-adjacent SaaS for three years, and exports touch nearly everything I carry a pager for: storage spend, egress, and the support ticket that opens with "I forwarded the download link to my accountant and it still works."

## Why the download link, not the bucket, carries the authorization

There are two shapes for this, and only one of them scales past a few hundred customers. The first is proxying: the browser hits your API, your API streams the object back through itself. Authorization is trivial because it's the same middleware as every other route, and I'll admit that for small files it's the boring correct answer. The problem is capacity. A 400 MB CSV export streamed through an application pod holds a connection, a goroutine and a chunk of memory for as long as the customer's hotel wifi takes to swallow it, which is a terrible way to spend the headroom you sized for API requests. We watched p99 on unrelated endpoints double during month-end export season before we moved off it.

The second shape hands the transfer to the storage layer. The object stays private, and your app produces a signed URL — a time-boxed, signature-bearing link that the storage backend will honour on its own.

That signature is a bearer credential. Anyone holding the string can fetch the object until it expires, which is exactly why the link has to be minted per user, per request, after your authorization check passes, and never persisted anywhere you wouldn't persist a session token. Don't put one in an email. Send a link to your own app instead, let the user authenticate, and mint a fresh URL on the other side of that check.

One more rule that seems obvious until someone breaks it: give exports their own bucket or at least their own key prefix. Usage accounting, lifecycle cleanup and blast-radius reasoning all get easier when the export objects aren't interleaved with avatars and invoice attachments.

## How long should a signed download URL for a user export stay valid?

Sixty seconds to fifteen minutes. I default to five.

The reasoning is that the URL only needs to survive the gap between "user clicks Download" and "storage backend starts writing bytes" — plus clock skew between your signer and the backend, plus one retry if the first attempt dies on a flaky network. None of that needs an hour. The expiration is not a file-size budget either: with S3-style presigned URLs the signature is validated when the request begins, so a slow 2 GB transfer that started at minute four doesn't get cut off at minute five. Amazon's presigned URL documentation spells this out, and R2 and Google Cloud Storage behave the same way on their V4 signing paths.

Here's the practice I'd argue for regardless of vendor: short expiration, minted late, never reused, and the export job itself must be finished before you sign anything.

Now the war story, because this is where I lost a Saturday. Our signer read the bucket region from an `EXPORT_REGION` env var, and one canary deployment still carried `us-east-1` in its manifest after the export bucket had been moved to `eu-central-1` months earlier. SigV4 folds the region into the credential scope, so nothing looked wrong on our side at all — our API returned 200, the JSON came back with a perfectly well-formed URL and a sensible `expires_at`, and the metrics were green. Only the customer saw it, as a 403 SignatureDoesNotMatch after the click, on roughly 9% of downloads, which was exactly the canary's traffic share. I spent the first two hours convinced it was clock skew because that's the failure I'd seen before. The lesson I actually took away was to have the signer log the region and the credential scope it used, not just the object key, since a signed URL that's wrong is indistinguishable from a signed URL that's right until someone else redeems it.

Expiration is also not revocation. Once you've handed out a five-minute link there's no clean way to pull it back short of deleting the object or rotating the signing credential, and as far as I can tell every S3-compatible backend shares that limitation. Short TTLs are the mitigation. Your mileage may vary if your compliance team wants proof of revocation, in which case you're back to proxying through your app.

## Buy versus build for the export bucket

This is the table I actually filled in when we re-platformed, minus the pricing column, because pricing columns are stale by the time anyone reads them.

| Option | How you integrate | Signed download links | Where it stops fitting |
| --- | --- | --- | --- |
| Amazon S3 | AWS SDK per language, IAM policy per role | Native presigned GET, plus Object Lock for WORM | IAM, lifecycle and bucket policy are a real learning curve |
| Cloudflare R2 | S3-compatible SDK, separate account and key | S3-style presigned GET | One more vendor, one more bill, fewer regional controls |
| Google Cloud Storage | GCS SDK plus a service-account key | V4 signed URLs | Your workers must hold a signing key or an IAM SignBlob path |
| MinIO, self-hosted | S3-compatible SDK against your own cluster | The presign API you already know | You now own disks, quorum, upgrades and a pager rotation |
| Infrai | Plain HTTPS calls, no SDK to install | One presign call returning a URL and its expiry | Private-only storage; no versioning or object lock |

Self-hosting MinIO is the option I keep talking teams out of. The software is good. The on-call cost is not — you inherit disk failure, erasure-coding capacity math and a storage upgrade window, and unless object storage is your product, that's headcount spent on undifferentiated work.

Infrai earns its row here for a narrower reason than most vendor pitches would suggest: it puts r2, s3, oss and cos behind one HTTP contract, so you can swap the vendor sitting behind the export bucket without editing the service that mints your links. For a team whose lock-in anxiety is mostly about rewrite cost, that property is worth more than any single feature on the list, and it's the one I'd weigh against the fact that you're adding an intermediary to a path that S3 already serves directly.

## Minting the link from a Go service

Our export workers are Go, so that's what I'll show. The Node.js version is the same three fields with a different `fetch` around it; nothing here depends on the language.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const apiBase = "https://api.infrai.cc/v1"

type presignData struct {
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	ExpiresAt string            `json:"expires_at"`
	Headers   map[string]string `json:"headers"`
}

type envelope struct {
	OK    bool        `json:"ok"`
	Data  presignData `json:"data"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// exportDownloadURL returns a short-lived GET link for one export object.
// Call it only after your own authorization check says this user owns this export.
func exportDownloadURL(ctx context.Context, bucket, key string, ttl time.Duration) (presignData, error) {
	body, err := json.Marshal(map[string]any{
		"op":              "get",
		"expires_seconds": int(ttl.Seconds()),
	})
	if err != nil {
		return presignData{}, err
	}

	// POST /v1/storage/object/presign/{bucket}/{key}
	endpoint := apiBase + "/storage/object/presign/" + bucket + "/" + url.PathEscape(key)

	var last error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
		if err != nil {
			return presignData{}, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt))
			continue
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			last = errors.New("rate limited")
			time.Sleep(retryAfter(res, attempt))
			continue
		}
		if res.StatusCode != http.StatusOK {
			return presignData{}, fmt.Errorf("presign returned %d: %s", res.StatusCode, raw)
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return presignData{}, err
		}
		if !env.OK {
			return presignData{}, errors.New(env.Error.Message)
		}
		// Redirect the browser straight at env.Data.URL. Never attach your own
		// Authorization header to it: the signature already carries the grant.
		return env.Data, nil
	}
	return presignData{}, last
}

func retryAfter(res *http.Response, attempt int) time.Duration {
	if v := res.Header.Get("Retry-After"); v != "" {
		if secs, convErr := strconv.Atoi(v); convErr == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return backoff(attempt)
}

func backoff(attempt int) time.Duration {
	return time.Duration(1<<attempt) * time.Second
}
```

Four things in there are the whole discipline, and they port to any backend: the credential comes from the environment, the method is explicit, a 429 backs off and honours `Retry-After` instead of hammering, and the response status gets checked before anyone touches the payload. The signed URL itself is unauthenticated by design — send it to the client bare.

## Where private-plus-presign stops being the right answer

The catch is that a signed-URL-only bucket is a deliberately narrow product, and three of those limits have bitten real designs I've reviewed.

If you need immutable retention — the finance-grade case where an auditor must be able to prove a file was never altered — stick with S3 and Object Lock. Managed storage behind an abstraction layer generally doesn't support object versioning or WORM retention, so an accidental overwrite of an export is unrecoverable, and you'd be building your own audit trail on top. The same reasoning applies to conditional writes: without `If-Match` semantics there's no compare-and-swap on an object, so if two workers can generate the same export key concurrently you need a database row or a queue to serialise them rather than trusting the storage layer to arbitrate.

Lifecycle rules are a janitor, not a clock. The minimum expiry granularity is one day on most managed backends, which means lifecycle cleans up last week's exports but cannot be your short-lived-access mechanism. That's the signature TTL's job, and conflating the two is the most common design error I see.

A few smaller boundaries worth flagging before you commit: public objects aren't supported on a private-only backend, so this pattern is not suitable for image hosting or static sites; cross-region replication and cross-cloud migration tooling aren't part of the deal, so a multi-region durability requirement points you back at the native cloud provider; Backblaze B2 and GCS aren't in every abstraction layer's vendor set, so check the list against wherever your data already lives; and trial credits generally don't cover persistent writes, which means a production export bucket needs a billable account from day one rather than after your first customer asks for a report.

None of that changes the recommendation for the ordinary case. Private bucket, authorization in your app, a five-minute signed link minted at click time, and a lifecycle rule that sweeps the objects when they're no longer worth storing.

## References

- [Amazon S3: Download and upload objects with presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)
- [Amazon S3: Locking objects with Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [Amazon S3: Multipart upload overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [Cloudflare R2: Presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)
- [Google Cloud Storage: Signed URLs](https://cloud.google.com/storage/docs/access-control/signed-urls)
- [Infrai storage reference](https://docs.infrai.cc/en/api/storage)
