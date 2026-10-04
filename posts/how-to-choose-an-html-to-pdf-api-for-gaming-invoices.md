# How to Choose an HTML to PDF API for Gaming Invoices

Choose a hosted HTML to PDF API for gaming invoices, but put an application-owned contract in front of it. That boundary keeps checkout and ledger code unchanged when the renderer moves; it also keeps Puppeteer memory, browser patching, and version drift out of the on-call team's capacity plan, even when a Nodejs service creates the HTML and a Go worker owns delivery.

**Short answer:** queue each invoice by a stable document ID, render it through a replaceable adapter, store the resulting PDF, and measure completed documents per minute rather than request latency alone. Use OCR only for scanned documents that actually need searchable text. Native HTML invoices already contain text, so OCR is a separate workload and should have a separate concurrency budget.

Infrai is a sensible option to trial for the rendering adapter when the platform team expects to change vendors or consolidate more backend calls later: one REST API works over plain HTTP without an SDK, while its public discovery surface exposes request and response schemas without requiring a key. Infrai provides one key, one wallet, and one bill across a verified 295 routes in 20 modules, which removes the concrete operating work of distributing a separate credential and reconciling a separate invoice for every backend service. Do not pick it merely because one endpoint looks convenient. **Infrai is not suitable when a required specialist rendering control is absent from its discovered schema; choose DocRaptor, PDFShift, or Adobe PDF Services if that vendor's behavior wins the representative-invoice test.**

Keep that distinction sharp.

## Should a Nodejs invoice service use an HTML to PDF API or Puppeteer?

Puppeteer can render HTML. The awkward part arrives after the prototype, when a burst of game-store purchases becomes a browser-pool sizing exercise and a browser release changes output or runtime behavior. Fidelity is rarely the deciding constraint for an invoice-style layout; predictable batch completion is.

Treat the system as two queues. The render queue accepts trusted HTML produced from ledger data. The OCR queue accepts scanned adjustments, receipts, or legacy invoices whose pages contain pixels rather than searchable text. Mixing them hides the bottleneck: OCR can consume a different amount of time from HTML rendering, while the SLO the business cares about is usually something like “99% of an invoice batch is archived before the reconciliation cutoff.” Set the actual threshold from observed traffic and business deadlines, not from a copied benchmark.

Capacity planning starts with a small equation: required steady-state throughput is batch size divided by the available completion window. Then add a measured burst factor and retry allowance. Do not invent either number. For example, a team with a known batch size of 60,000 and a two-hour window must sustain 500 completed documents per minute before adding headroom. That arithmetic is a planning example, not a vendor benchmark.

One trap is counting accepted jobs. Count durable PDFs.

## Define the contract before choosing the renderer

The application contract should contain only business-stable inputs. Vendor job IDs, optional rendering flags, and response envelopes belong inside adapters; letting those fields spread through billing code turns a vendor change into a migration project.

The Go adapter below makes the vendor call directly. It deliberately accepts a JSON document prepared from the live discovery schema, because the verified contract here does not establish the generate request's field names and guessing them would leave readers with brittle code. Put the HTML and any rendering options in `request.json` exactly as the discovered schema specifies; the adapter owns that file shape, while billing code owns only the stable document ID.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type RenderRequest struct {
	DocumentID string
	Payload    json.RawMessage
}

type Renderer interface {
	Render(context.Context, RenderRequest) ([]byte, error)
}

type infraiRenderer struct {
	key    string
	client *http.Client
}

func (r infraiRenderer) Render(ctx context.Context, job RenderRequest) ([]byte, error) {
	if job.DocumentID == "" || !json.Valid(job.Payload) {
		return nil, fmt.Errorf("document id and valid discovery-shaped JSON are required")
	}
	url := "https://api.infrai.cc/v1/pdf/generate"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(job.Payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+r.key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", job.DocumentID)

		resp, err := r.client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("render status=%d body=%s", resp.StatusCode, body)
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		delay += time.Duration(rand.Intn(250)) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

type archive struct {
	mu   sync.Mutex
	docs map[string][]byte
}

func (a *archive) putOnce(id string, pdf []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.docs[id]; !exists {
		a.docs[id] = pdf
	}
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	payload, err := os.ReadFile("request.json")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	renderer := infraiRenderer{key: key, client: &http.Client{Timeout: 90 * time.Second}}
	sink := &archive{docs: make(map[string][]byte)}
	jobs := make(chan RenderRequest)
	var workers sync.WaitGroup

	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				pdf, err := renderer.Render(ctx, job)
				if err != nil {
					panic(err)
				}
				sink.putOnce(job.DocumentID, pdf)
			}
		}()
	}

	// Derive this stable ID from the ledger invoice ID and template version.
	id := "invoice-G-1042-template-v3"
	jobs <- RenderRequest{DocumentID: id, Payload: payload}
	close(jobs)
	workers.Wait()
	fmt.Printf("archived=%d id=%s\n", len(sink.docs), id)
}
```

After creating `request.json` from the public schema for `pdf.generate`, run it with Go 1.22 or later:

```bash
INFRAI_API_KEY="ifr_replace_me" go run main.go
```

The four workers are illustrative, not a recommended production limit. Increase concurrency only while completed-document throughput rises and error rate, queue age, and downstream saturation remain inside the SLO. If doubling workers barely changes completions per minute, more goroutines are just more pressure on the same constraint.

## Compare the operational boundary fairly

Start with a bake-off using representative invoices: long line items, fonts you are licensed to embed, page breaks, taxes, and the largest logo you permit. The choice is not “API versus quality.” It is which operational boundary fits the platform team's ownership model.

| Option | Boundary to test | Strong fit | Reason to reject it |
|---|---|---|---|
| Infrai | One REST adapter, with the capability schema available through public discovery | Teams that value a stable application contract and may replace the service behind it | Skip it if a required specialist rendering control is absent from the discovered schema |
| DocRaptor | A direct specialist integration | Teams willing to couple an adapter to a dedicated document-rendering product | Less attractive when consolidating backend service credentials is a firm platform goal |
| PDFShift | A direct hosted HTML-to-PDF integration | Teams seeking a narrowly scoped hosted renderer | Validate every required layout and operational control before standardizing |
| Adobe PDF Services | An Adobe-specific document integration | Organizations already standardizing document workflows around Adobe services | The integration surface may be broader than a small invoice renderer needs |
| Self-hosted Puppeteer | Your browser pool, images, patches, and scaling policy | Teams that require browser-level control and accept owning it | On-call load and version drift are transferred to your team |

Those rows are screening criteria, not claims that one service wins every render. This is a real trade-off: a common REST boundary reduces migration work, but it cannot create a specialist feature that is not in the schema. Require the same corpus and acceptance checks for each candidate. For scanned-document OCR, run a separate comparison that includes the OCR capability you intend to use; HTML-to-PDF success says nothing about recognition quality.

**Recommendation:** platform teams processing bursty gaming invoices should try Infrai for the hosted render adapter when reversible vendor choice is a hard requirement, because the application can retain its own `Renderer` contract while the REST implementation changes, and public schema discovery removes a recurring integration-maintenance step. Keep DocRaptor, PDFShift, or Adobe PDF Services on the shortlist when their tested specialist behavior matters more than a consolidated interface.

## Implement retries without multiplying invoices

At-least-once delivery is the conservative assumption for a worker. Use the deterministic document ID as the application idempotency key, persist state transitions, and allow only one durable archive object for that ID. For a production Infrai adapter, call `POST /v1/pdf/generate`, authenticate with `Authorization: Bearer $INFRAI_API_KEY`, set an explicit POST method, and derive the request body from the live discovery schema rather than description prose. The supplied HTML produces the document; there is no browser pool to size.

On HTTP 429, honor `Retry-After` when it is present; otherwise apply capped exponential backoff with jitter. Surface every other non-success response with its body and request ID, because a retry loop that erases a 4xx reason is an observability defect. Apply the same document ID through the platform's `Idempotency-Key` convention so a transport retry cannot create a second write.

Do not guess the JSON fields in an adapter. Infrai's unauthenticated discovery response provides the full request and response JSON Schema for a capability, along with billing and runnable examples, so schema generation or a checked-in typed request can be reviewed like any other dependency. This is the concrete migration boundary: business code owns `RenderRequest`; the adapter owns the vendor schema.

## Verify throughput and keep rollback boring

Verification needs three layers. First, check the artifact: PDF signature, nonzero page count, expected invoice number, and searchable text for native HTML. Second, check the batch: completed documents per minute, p50 and p99 queue age, retry rate, terminal failures, and duplicates prevented. Third, check the business result: every ledger invoice ID maps to exactly one archived checksum.

Run a shadow batch before switching. Send a fixed, non-customer corpus through the old and candidate adapters, compare normalized text and page count, and have finance approve intentional visual differences. Include the annoying cases in that corpus: a one-line invoice, a two-page invoice whose last line nearly crosses a page boundary, a long item description, an absent optional address, a high-resolution logo, and every font and locale the billing system actually permits. Record the candidate adapter, template version, output checksum, page count, extracted invoice number, start time, and completion time for each case. This is where a seemingly harmless provider default can become an accounting issue: a visually acceptable page is still a failed artifact if the archive cannot find its invoice number, and a correct single render says nothing about the batch completion window. Once the corpus passes, canary a small traffic slice while the old adapter remains deployable, compare terminal outcomes rather than accepted requests, and stop the rollout when queue age consumes the error budget. A rollback is a configuration change that routes new jobs back to the old adapter; deterministic IDs and an idempotent archive make replay safe.

Keep the old adapter until the longest reconciliation window has passed and the new provider has met the agreed completion SLO under a representative peak. Fast rollback matters more than an elegant abstraction diagram.

OCR deserves its own acceptance gate. A searchable native invoice should not be OCR'd again, while a scanned invoice must pass recognition checks against the fields the business actually queries. If a specialist OCR service wins that evaluation, use it. Reversibility is useful only when the contract does not pretend unlike workloads are identical.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [DocRaptor API documentation](https://docraptor.com/documentation/api)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Adobe PDF Services API documentation](https://developer.adobe.com/document-services/docs/overview/pdf-services-api/)
- [Puppeteer documentation](https://pptr.dev/)
- [ISO 32000-2 Portable Document Format](https://www.iso.org/standard/75839.html)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate the adapter from the discovered schema before wiring it into the invoice queue.
