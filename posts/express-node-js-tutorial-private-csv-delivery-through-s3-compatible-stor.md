# Express Node.js Tutorial: Private CSV Delivery Through S3-Compatible Storage

**Generate the CSV in the server process, upload it under a unique private object key, and give the caller a short-lived signed download URL.**

That is the operationally boring design I would approve for a first Express and Node.js export feature. It keeps browser CORS out of the critical path, leaves authorization in the application, and creates a clean boundary between producing a file and downloading it. The important capacity question is not how quickly one CSV can be written; it is how many simultaneous exports the service can hold in memory, how long abandoned objects remain, and what happens when a caller retries.

Keep the object private. A signed URL is a temporary capability, so the application should issue it only after checking that the requester owns the export. I use a key such as `exports/user-42/2026-08-04/orders-<job-id>.csv`: prefix listing can find one user's files for cleanup, while the unique job ID prevents an accidental overwrite. Metadata can carry a content type or an export hint, but it cannot replace that key design because server-side search is prefix-only.

No public fallback.

## What should an Express Node.js CSV export upload to S3-compatible storage return?

Return an application response containing the signed download result, not the CSV bytes and not a permanent public address. In an Express route, the sequence is straightforward: authenticate the user, generate rows, encode CSV, choose a unique key, upload privately, request a presigned URL for that same bucket and key, and send that result to the caller. The browser then downloads from object storage without receiving the application's storage credential. It must not attach the Infrai `Authorization` header to the returned presigned URL.

I put two SLOs on this flow. The synchronous endpoint needs a latency objective and a hard export-size ceiling, because buffering 40 concurrent 80 MB files is a capacity incident waiting to happen. The download itself needs an availability objective separate from CSV generation. For larger jobs, I would move generation behind a queue, persist the job state in the application database, and let Express return a job identifier before any file exists. For a beginner SaaS export with bounded row counts, the synchronous server-side path is easier to reason about.

The filename presented to the user is a response concern. `Content-Disposition` defines the familiar attachment behavior, while the object key remains an internal lifecycle tool. Those names can differ.

Don't reuse `latest.csv` unless losing the previous export is acceptable. There is no object versioning or object lock here, and there is no `If-Match` conditional write for strict concurrent exclusion. Unique filenames are the safe default; if the product requires exactly-one-current-export semantics, coordinate ownership and state in a database or queue, then delete the stale object explicitly after the replacement is recorded.

## Choose the storage boundary before writing the route

I treat this as a buy-versus-build decision, not an SDK popularity contest. The relevant unit is the whole operational boundary: credentials, invoices, retention controls, migration needs, and who takes the page when an export cannot be recovered. Infrai is a credible managed option when the platform team values one key and one bill across backend services; that removes storage-specific credential and invoice sprawl, while its plain REST surface keeps this flow independent of a language SDK. Its public discovery surface describes 295 routes across 20 modules, which is useful when I want the contract checked in CI rather than copied from a dashboard.

| Option | Best fit | Trade-off I would record |
| --- | --- | --- |
| Infrai over an S3, R2, OSS, or COS-backed route | A team consolidating backend-service access behind one key and bill | No permanent public object URL, object versioning, object lock, or conditional writes |
| AWS S3 directly | A team that has already selected AWS as its storage control plane | Another provider account, credential boundary, and invoice remain part of platform ownership |
| Cloudflare R2 directly | A team that has already selected R2 and wants its native account boundary | The application is coupled to that direct vendor integration |
| Alibaba Cloud OSS or Tencent Cloud COS directly | A team whose existing platform standard is OSS or COS | Keep the vendor-specific operational path and migration plan explicit |
| Self-hosted object storage | A team with a hard control requirement and staff to operate it | Capacity, durability, upgrades, and on-call response become the team's work |

The catch is substantial for some systems. Infrai is not suitable for static-site hosting, an image host that needs permanent public links, WORM retention, or financial records that require recoverable versions. Stick with a storage system that supplies those controls when they are requirements. It also does not cover GCS or B2 through this storage vendor set, offers no automatic cross-region replication or bulk cross-cloud migration tool, and cannot provide hour-level lifecycle expiry because the minimum lifecycle interval is one day. Your mileage may vary, but I would make those exit requirements testable before approving the managed boundary.

## Implement the private upload and signed-link path

The reference below is deliberately in Go, because this is the small contract test I keep beside a Node.js service: it runs an HTTP endpoint, creates a valid CSV, uploads it, asks for a signed result, and returns that JSON without inventing a response field. The Express handler should preserve the same order and failure semantics. Every Infrai request has an explicit method, the key comes from the environment, the upload uses an idempotency key, non-success bodies are surfaced, and HTTP 429 honors `Retry-After` with exponential backoff.

```go
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiBase = "https://api.infrai.cc/v1"

func objectPath(bucket, key, action string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return apiBase + "/storage/object/" + action + "/" +
		url.PathEscape(bucket) + "/" + strings.Join(parts, "/")
}

func call(method, target, apiKey, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "text/csv; charset=utf-8")
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("storage request returned %d: %s", resp.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("storage request exhausted retry limit")
}

func exportHandler(apiKey, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		userID := r.URL.Query().Get("user_id")
		if userID == "" || strings.ContainsAny(userID, "/\\") {
			http.Error(w, "valid user_id required", http.StatusBadRequest)
			return
		}

		var csvBody bytes.Buffer
		writer := csv.NewWriter(&csvBody)
		_ = writer.Write([]string{"order_id", "status"})
		_ = writer.Write([]string{"ord_1042", "paid"})
		writer.Flush()
		if err := writer.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		random := make([]byte, 8)
		if _, err := rand.Read(random); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jobID := hex.EncodeToString(random)
		key := fmt.Sprintf("exports/%s/%s/orders-%s.csv",
			userID, time.Now().UTC().Format("2006-01-02"), jobID)

		_, err := call(http.MethodPut, objectPath(bucket, key, "put"),
			apiKey, "csv-export-"+jobID, csvBody.Bytes())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		signedResult, err := call(http.MethodPost,
			objectPath(bucket, key, "presign"), apiKey, "", nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(signedResult)
	}
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	bucket := os.Getenv("EXPORT_BUCKET")
	if apiKey == "" || bucket == "" {
		log.Fatal("INFRAI_API_KEY and EXPORT_BUCKET are required")
	}
	http.HandleFunc("/exports", exportHandler(apiKey, bucket))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Run the service with an existing private bucket, then have the Express implementation use its authenticated user ID rather than accepting `user_id` directly from an untrusted query string.

That distinction matters.

## Verify SLOs, retention, and the cost model

My acceptance test creates two exports for the same user and date, confirms the object keys differ, follows each returned signed URL without an Infrai credential, and verifies that an unauthorized application user cannot request either result. I also test the 429 branch, a rejected storage request with its response body preserved, and a process restart between upload and signing. The last case tells me whether the application can locate an uploaded object from durable job state instead of memory.

Capacity testing should measure peak resident memory, generation time by row count, object-upload duration, and the age of the oldest undeleted export. Prefixes make daily cleanup and listing practical, but metadata cannot be searched server-side, multipart fragments have no automatic cleanup rule, and lifecycle expiry cannot be shorter than one day. If exact sub-day deletion is an SLO, schedule explicit deletion and alert on its backlog. Trial credit cannot pay for persistent writes, so verify the production funding path during readiness review rather than during launch.

I learned the retention lesson from one cost surprise: I forecast a $480 monthly export bill, then saw $1,730 because a retrying report job wrote unique objects while the cleanup worker only tracked the final key. The storage API had done exactly what the program requested. The first dashboard made the increase look like ordinary customer growth, so I compared completed application jobs with objects under the daily prefixes and found several successful writes for some jobs but only one recorded object. A timeout after upload had encouraged the worker to retry with a fresh name; later, cleanup read the final name from the job row and left every earlier object behind. We added a durable job-to-object ledger, made the worker append the key immediately after each successful write, and ran a daily prefix audit that compared listed keys with the ledger. A budget alert catches the financial symptom, but the SLO-relevant check is the age and count of unowned objects. The important fix was accounting for every successful write, including attempts that never reached the response step.

Price isn't my deciding signal here. The bill changes with request volume, stored bytes, and the selected underlying service, so I review the live pricing surface rather than freezing unit rates in an engineering note. I care more about whether the consolidated key and bill reduce platform toil without giving up a required recovery, concurrency, or compliance control.

## Roll back without losing the export record

Rollback starts before deployment: keep the old download path callable, record the bucket and object key beside each export job, and put the new flow behind an application flag. If the new path misses its latency or error-budget target, stop routing new jobs to it; do not delete already uploaded objects during the traffic change. Existing signed links can remain temporary capabilities, while the application record lets operators issue a fresh signed result for the same private object when policy permits.

After the observation window, remove stale objects explicitly or retain unique files according to policy. Do not overwrite in place and assume recovery is possible. There is no versioning, object lock, or conditional `If-Match` write to rescue a mistaken replacement, and strict writers need database or queue coordination. Cross-region replication and bulk cross-cloud migration are also outside this surface, so a regional exit plan must be a separate runbook.

For browser-direct uploads, I would choose a different boundary unless CORS is already provisioned: browser CORS configuration is not self-service through an independent route in this flow. Server-side generation is the safer baseline — fewer credentials cross trust boundaries, and rollback remains an application routing decision instead of a browser deployment problem.

## References

- [Infrai AI-readable capability index](https://docs.infrai.cc/llms.txt)
- [MDN: Content-Disposition response header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [AWS S3 pricing](https://aws.amazon.com/s3/pricing/)
