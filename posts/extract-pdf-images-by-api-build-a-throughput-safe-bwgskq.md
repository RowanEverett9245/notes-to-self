# Extract PDF Images by API: Build a Throughput-Safe Freight Thumbnail Gallery

A logistics document-preview service should produce two different artifacts: extract embedded images when operators need the originals, and render each page when they need a faithful thumbnail of the filled, flattened form. Do not ask one representation to serve both jobs. The least complex reliable design records both against the source page in a manifest, then lets the gallery choose deliberately.

**TL;DR:** Page renders are the default for visual review; embedded assets are an opt-in lane for inspecting labels, stamps, or photographs at their native representation. Batch throughput, storage fan-out, and source-page attribution belong in the acceptance criteria before vendor selection.

The page fires at 02:17. On-call sees a freight-document queue whose completion rate has fallen below its service-level objective while workers remain busy, followed by a preview gallery with missing context: several extracted logos and signatures exist, but nobody can tell which page supplied them. CPU is not the useful first clue. The useful clue should have arrived earlier: output assets per input page climbed, object writes multiplied, and the oldest batch stopped making progress toward its deadline.

## Why did the alert arrive after the gallery was already late?

A request counter treats every PDF as roughly equivalent. A shipping packet disproves that assumption quickly. One file may be a compact flattened delivery form; another may contain many embedded assets across bills of lading, customs sheets, and proof-of-delivery pages. Extraction can return many assets, so input-document rate alone conceals the work amplification that consumes storage bandwidth and stretches the queue.

The earlier signal is a batch burn-rate view built from units the user actually waits for: pages rendered, assets extracted, object writes completed, and oldest-job age. Track the distribution of `assets_per_page`, not merely its average. Then alert when the batch is consuming its latency budget faster than the SLO permits while completed output units flatten. This does not claim a universal threshold; the threshold must come from the fleet's own load tests and error budget.

Short spikes happen.

Paging on each one converts a capacity signal into on-call noise. A defensible policy needs a short window that detects acute exhaustion and a longer window that confirms sustained budget burn, with the page reserved for cases that threaten the promised batch deadline.

## Should an API extract PDF images or render them for a thumbnail gallery?

Embedded-image extraction and page rendering answer different questions. Extraction asks, "Which image objects can I recover from this document?" Rendering asks, "What did page 7 look like after the carrier form was filled and flattened?" A gallery intended for operational review normally needs the second answer because page composition, text, marks, and image placement are part of what the operator sees. An evidence workflow that needs the original photograph or stamp may need the first.

The manifest should make that distinction impossible to lose. Give every output a stable document ID, source page number, artifact kind (`page_render` or `embedded_image`), object key, media type, byte count, and processing status. If extraction cannot attribute an asset to its source page, it is a poor fit for this gallery contract even if the pixels are pristine. Attribution is part of correctness.

A compact Go type makes the contract reviewable:

```go
package gallery

type Artifact struct {
	DocumentID string `json:"document_id"`
	SourcePage int    `json:"source_page"`
	Kind       string `json:"kind"`
	ObjectKey  string `json:"object_key"`
	MediaType  string `json:"media_type"`
	Bytes      int64  `json:"bytes"`
	Status     string `json:"status"`
}

type DocumentManifest struct {
	DocumentID string     `json:"document_id"`
	Artifacts  []Artifact `json:"artifacts"`
}
```

Keep filled and flattened PDFs as immutable inputs to preview generation. That makes retries observable and prevents a later form mutation from silently changing what a previously approved thumbnail represented. It also lets the worker deduplicate by document version plus artifact kind, rather than guessing whether two deliveries with the same filename are identical.

## Instrument the amplification, then size the queue

Capacity planning starts with output fan-out. For each batch, record input documents, input pages, rendered pages, extracted assets, total output bytes, object-write attempts, successful writes, retries, and end-to-end batch age. Histograms for pages per document, assets per page, and bytes per artifact expose the tail that determines worker memory and storage demand. Counters alone do not.

A useful planning equation is intentionally plain: required steady-state output capacity must exceed arrival rate multiplied by the high-percentile output fan-out, with headroom for retries and recovery after a worker outage. The actual percentile and headroom are operational choices, not numbers that can be borrowed from a vendor page. Validate them with representative freight packets, including image-heavy scans and multi-page flattened forms.

Backpressure belongs at admission. When object writes or downstream rendering cannot keep pace, stop expanding new PDFs into dozens of child tasks; preserve queue-age visibility and resume when capacity returns. Blind concurrency increases memory pressure and retry volume precisely when the batch is already missing its objective.

This is the instrumentation change that should precede production rollout: page on sustained SLO burn, ticket on a fan-out distribution shift, and retain a dashboard split by artifact kind. If page renders are healthy while extraction is saturated, operators can keep the review gallery available and defer the optional originals lane. That is a useful degradation boundary.

## Buy-versus-build choices under batch load

The shortlist should be tested with the same document corpus and manifest contract. Adobe PDF Services, PDF.co, Nutrient, and Apryse are managed or SDK-based document products. Infrai fits teams that want a plain REST API with no client SDK to install or version; its verified PDF operations include image extraction and conversion. The public discovery surface can describe request and response schemas without a key, and the platform spans 295 routes in 20 modules under one key. For a polyglot logistics platform, that reduces credential and client-library sprawl, but it does not remove the need to measure fan-out or validate source-page attribution.

The following Go program verifies that the advertised extraction path is present in live discovery. It is deliberately a discovery check rather than a fabricated processing request: the request body schema must come from that discovery record.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type discovery struct {
	Capabilities []capability `json:"capabilities"`
}

func main() {
	baseURL := os.Getenv("PDF_API_BASE_URL")
	if baseURL == "" {
		panic("PDF_API_BASE_URL is required")
	}
	req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
	if err != nil {
		panic(err)
	}
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("discovery returned %s", resp.Status))
	}
	var catalog discovery
	if err := json.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		panic(err)
	}
	for _, item := range catalog.Capabilities {
		if item.Method == http.MethodPost && item.Path == "/v1/pdf/extract_images" {
			fmt.Println(item.Method, item.Path)
			return
		}
	}
	panic("image extraction capability not found")
}
```

| Option | Ownership boundary | Batch-throughput question to prove | Best fit |
|---|---|---|---|
| Adobe PDF Services | Managed document API and vendor SDKs | Do completion and output retrieval sustain the batch deadline? | Teams comfortable with Adobe's managed workflow |
| PDF.co | Managed PDF API | How do image-heavy packets affect child outputs and queue age? | Teams wanting a broad hosted PDF API |
| Nutrient | Document SDK and server tooling | Which deployment gives enough worker control without excess on-call load? | Teams needing deeper document integration |
| Apryse | Document SDK and server-side portfolio | Can the deployment meet concurrency and memory limits on the test corpus? | Teams prioritizing a broad document SDK surface |
| Infrai | Plain REST boundary with one platform key | Do extraction and conversion satisfy the manifest and attribution contract? | Polyglot services that value a consistent HTTP interface |
| Self-hosted engine | Platform owns scaling, patching, and incidents | Is peak control worth permanent operational ownership? | Isolated environments staffed to run the control plane |

DocRaptor, PDFMonkey, and PDFShift focus on document generation rather than this embedded-image recovery decision, so they are not suitable substitutes when the requirement is to extract originals from incoming freight PDFs. Gotenberg, WeasyPrint, and wkhtmltopdf likewise belong on a generation shortlist, not an extraction shortlist. Naming a familiar PDF product does not make it relevant.

The table is a test plan, not a winner board. The operational trade-off is explicit: managed services reduce components the platform team operates, but introduce a provider boundary and possible lock-in around job semantics. Self-hosting gives tighter capacity control and can keep documents inside a chosen environment, while shifting upgrades, security response, queue behavior, and scaling onto the on-call rotation. A limitation of the managed REST choice is that it is not suitable when policy requires an isolated self-hosted processor, or when its discovered schema cannot meet the attribution contract; choose a self-hosted engine or a document SDK whose deployment model does. I would reject any option that cannot return enough metadata to build the manifest, regardless of nominal concurrency.

Run a bake-off with fixed input hashes and count output units, bytes, retries, and completion time per batch. Do not publish a single average. Compare tail completion time and recovery after deliberate worker interruption, because a system that is fast only in the clean run does not protect a logistics cutoff.

## Make the decision at the gallery boundary

Use rendered pages for the primary freight-review gallery. They preserve what the operator is meant to inspect after form filling and flattening. Enable embedded extraction only for workflows that explicitly need original assets, store those outputs under a separate artifact kind, and require source-page attribution.

Then write the SLO around the visible outcome: a batch submitted before a logistics cutoff becomes reviewable within the agreed window. The supporting service-level indicators are oldest-job age, completed page renders, extraction fan-out, and durable object writes. Vendor request success is diagnostic; it is not the user outcome.

There is a cost to setting the alert too low. Image-heavy but healthy packets will page on-call, responders will learn that fan-out alerts are harmless, and the one sustained burn that threatens the cutoff will receive slower attention. Set the threshold from load-test distributions, pair it with queue-age budget consumption, and revisit it when the document mix changes.

Quiet is not the goal. Actionable is.

## References

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- Adobe PDF Services API documentation: https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- PDF.co API documentation: https://apidocs.pdf.co/
- Nutrient documentation: https://www.nutrient.io/guides/
- Apryse documentation: https://docs.apryse.com/
- DocRaptor documentation: https://docraptor.com/documentation/
- Gotenberg documentation: https://gotenberg.dev/docs/getting-started/introduction

## Further reading

- PDF Association technical resources: https://pdfa.org/resource/
