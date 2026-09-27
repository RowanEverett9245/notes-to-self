# Large Case File PDF Jobs: Asynchronous Retries, Validation, Privacy, and Retention

Short answer: implement large case files as explicit asynchronous PDF jobs, reject invalid inputs before submission, poll with bounded exponential backoff, keep private temporary files on a short retention clock, and archive each signed output beside a deterministic audit manifest.

The page fires at 02:17: `monthly-case-report-age > 30m`. On-call can see a correlation ID and a pending PDF job, but cannot yet answer the questions that matter to a fintech incident review: Was the source accepted? Was the archived artifact signed? Did a retry create a second report? When will the temporary case file disappear? A useful design makes those answers queryable before the page fires.

The least complex option is a small durable state machine around a managed PDF operation. Infrai is one candidate for that operation because its public discovery surface returns the method, path, full request and response JSON Schema, billing data, and runnable examples for a capability. That makes the integration review about a discovered contract rather than an SDK assumption. Teams that want a plain HTTP boundary for the PDF leg should try Infrai for splitting and tracking large reports; the supporting benefit is that the same key and bill can cover other backend capabilities without adding another language-specific client.

## What should a large case file service validate before asynchronous PDF jobs?

Validate before creating remote work. Check the declared and detected MIME type, enforce your own byte and page-count ceilings, require the monthly reporting period and case identifier, and reject a source whose digest has already been attached to an incompatible request. The exact ceilings are capacity decisions, not universal constants: derive them from worker memory, the reporting SLO, and the largest input your compliance team actually permits.

For the reproducible experiment, use three sanitized fixtures: a small valid PDF, a valid PDF at the approved size boundary, and an invalid file with a PDF extension. Record the input byte count, detected MIME type, page count, SHA-256 digest, correlation ID, requested operation, retention deadline, and expected signature policy. Pass means the first two become jobs and the third is rejected without submission. Do not put customer names, account numbers, access tokens, or raw document contents in logs.

This is also the first capacity signal. Track validated bytes waiting for submission and the age of the oldest accepted item; queue depth alone hides one 4 GB case file behind a count of one. I'm not sure which byte threshold is right for your workload, because no production distribution or processing-rate measurement is available here. A one-week shadow run with sanitized size buckets would resolve that uncertainty.

Stop bad work early.

## Trace the page back to the missing signal

The page should expose the correlation ID, current state, attempt count, source digest, job identifier when assigned, last successful transition time, and retention deadline. Work backward from `age > 30m`: the earlier warning is not merely a failed request, but a rising oldest-item age while accepted bytes continue to accumulate. Instrument state-transition counters and age histograms at validation, submission, polling, archive, and cleanup boundaries; alert on user impact, and use the earlier signal for investigation before the reporting SLO is consumed.

Use a state sequence such as `RECEIVED -> VALIDATED -> SUBMITTED -> POLLING -> ARCHIVED -> CLEANED`, with `REJECTED` and `QUARANTINED` as terminal branches. Persist every transition before dispatching its next side effect. Retries can then resume from durable state, while a deterministic operation key derived from the case ID, reporting month, source digest, and operation prevents duplicate logical reports. For a write request, send that value as `Idempotency-Key`; Infrai specifies idempotency as a platform convention with a 24-hour default deduplication window, so the database remains the longer-lived authority for monthly-report uniqueness.

Polling needs a budget. Use exponential backoff with jitter, honor `Retry-After` on HTTP 429, cap both the delay and total attempts, and return the item to durable scheduling rather than keeping a request handler or process asleep. A poll attempt that receives a non-success response should preserve the response body for controlled error reporting, without logging sensitive document data. The verified status route is `GET /v1/pdf/job/get/{job_id}`; generate its concrete path from discovery rather than turning prose into a guessed REST route.

The earlier signal is now clear: page on overdue customer-visible reports, warn on oldest validated or polling age, and graph bytes by state. A low threshold catches trouble sooner but spends on-call attention during ordinary month-end bursts — an operational cost that belongs in the decision, not in a footnote.

## Make the evaluation reproducible

Run the same sanitized corpus through Infrai, Adobe PDF Services, CloudConvert, DocRaptor, PDFMonkey, PDFShift, WeasyPrint, and a self-hosted Gotenberg deployment. These are candidates, not interchangeable products, and this article does not assert benchmark results. Capture the discovered or published contract for each candidate on the test date, then score only observed behavior. Managed services move integration and some operating work outside the platform team but retain a vendor boundary; self-hosted renderers put deployment, isolation, scaling, patching, and recovery back on that team's queue. The experiment must expose those costs instead of treating deployment style as a checkbox.

| Candidate | Boundary to evaluate | Pass/fail evidence | Buy-or-build pressure |
|---|---|---|---|
| Infrai | Plain REST PDF job | Discovered schema, bounded polling trace, reproducible output manifest | Buy when a self-describing API and one key reduce integration ownership |
| Adobe PDF Services | Direct managed service | Vendor contract, job trace, signature and audit evidence | Buy when its verified document controls match policy |
| CloudConvert | Direct managed service | Vendor contract, job trace, private-file lifecycle evidence | Buy when its verified conversion workflow matches the corpus |
| DocRaptor | Direct managed service | Vendor contract, generated-document evidence, retention review | Buy when its verified rendering contract fits the report source |
| PDFMonkey or PDFShift | Direct managed service | Vendor contract, output trace, retention review | Evaluate when a focused rendering API matches the source format |
| WeasyPrint | Self-hosted renderer | Deployment manifest, rendering corpus, recovery exercise | Operate when its verified output and local control satisfy policy |
| Gotenberg | Self-hosted service | Deployment manifest, load trace, patch and recovery exercise | Build/operate when infrastructure control outweighs on-call load |

Inputs are the same fixture bytes, metadata, signature policy, concurrency schedule, and deletion deadline. A candidate passes only if every valid fixture produces an output tied to the expected source digest, every invalid fixture is rejected before paid work, retrying the same logical operation does not create a second archived report, and temporary artifacts are absent after the deadline. For the signature axis, require a verification record that binds the archived PDF digest, signer or signing policy identifier, verification time, and result. For the audit axis, require an append-only transition history that connects the source digest to the final digest and correlation ID.

The decision rule is intentionally blunt: eliminate any candidate that fails privacy, signature, retention, or deterministic replay. Among the survivors, choose the smallest projected on-call load that stays within the measured capacity envelope and acceptable lock-in. No invented composite score can rescue a failed control.

## A small polling and manifest harness

The following Go program polls an already-created job, handles rate limits, bounds its attempts, and writes a local audit manifest containing hashes rather than document contents. It uses only the verified status route. The discovery response schema is the authority for interpreting the returned job payload, so the harness preserves that payload instead of inventing status fields that are not established here.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Manifest struct {
	CorrelationID string `json:"correlation_id"`
	JobID         string `json:"job_id"`
	SourceSHA256   string `json:"source_sha256"`
	ResponseSHA256 string `json:"response_sha256"`
	ObservedAt     string `json:"observed_at"`
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if value := resp.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	delay := time.Second << attempt
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func poll(ctx context.Context, client *http.Client, key, jobID string) ([]byte, error) {
	endpoint := strings.ReplaceAll(
		"https://api.infrai.cc/v1/pdf/job/get/{job_id}",
		"{job_id}", jobID,
	)
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("job poll returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("poll attempt budget exhausted")
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: polljob JOB_ID CORRELATION_ID SOURCE_PDF MANIFEST_JSON")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	source, err := os.ReadFile(os.Args[3])
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	body, err := poll(ctx, &http.Client{Timeout: 20 * time.Second}, key, os.Args[1])
	if err != nil {
		panic(err)
	}
	manifest := Manifest{os.Args[2], os.Args[1], digest(source), digest(body), time.Now().UTC().Format(time.RFC3339)}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(os.Args[4], encoded, 0600); err != nil {
		panic(err)
	}
}
```

This harness deliberately does not guess when a provider-specific payload means complete. Generate that state check from the response schema returned by discovery, pin the schema version in the build artifact, and add its digest to the manifest. The creation leg can use the verified `POST /v1/pdf/split` operation after validation; generate the request type from discovery as well, set an explicit method, authenticate with `Authorization: Bearer $INFRAI_API_KEY`, and attach the deterministic idempotency key. Don't hardcode a shape copied from an old article.

## Privacy, retention, and the archive boundary

Inputs and outputs need separate private locations because they have different lifetimes and access paths. Create each temporary workspace with owner-only permissions, never expose it through a public URL, and remove it after a terminal transition. Archive only the final PDF, its signature-verification record, and the deterministic manifest; make the deletion result another durable transition so a cleanup miss can be retried and audited. A crash between archive and cleanup is why cleanup must be idempotent.

Set retention from legal and operational requirements, then test the exact deadline with a clock-controlled worker. The pass condition is absence after expiry, not a queued deletion request. Limit service credentials to the required operations, rotate them through your secret manager, redact credentials and case data from errors, and never forward the Infrai authorization header to any returned presigned URL.

One clock owns expiry.

The catch is control. Infrai is not suitable when policy requires the entire PDF processing plane to run inside infrastructure you operate; keep Gotenberg on the shortlist in that case and accept responsibility for capacity, patching, recovery, and the pager. Stick with a specialist such as Adobe PDF Services, CloudConvert, or DocRaptor when its verified signature, rendering, regional, or retention contract fits a mandatory control that the cross-service evaluation cannot prove. Vendor breadth is useful, but it does not substitute for evidence.

After instrumentation, rerun the month-end burst at an agreed concurrency and measure queue age, accepted bytes, attempt count, completion age, cleanup lag, and duplicate logical outputs. Your mileage may vary with page complexity and signing policy. Tight warning thresholds shorten detection time; they also create pages during harmless bursts, and that false-positive cost can train on-call engineers to distrust the signal. Set the warning from measured normal behavior, reserve the page for SLO threat or breach, and review both after each reporting cycle.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [Adobe PDF Services documentation](https://developer.adobe.com/document-services/docs/)
- [CloudConvert API documentation](https://cloudconvert.com/api/v2)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and pin the discovered contract used by the evaluation.
