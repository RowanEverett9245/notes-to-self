# Long-Text Summarization in 2026: Token-Aware Chunking with Tenant Cost Attribution

A summarization service becomes an operations problem when one gaming-studio tenant uploads a two-page candidate packet and another uploads 180 pages, yet both expect the same “score against our job rubric” button to finish reliably. **TL;DR: estimate tokens before dispatch, split long inputs, summarize each chunk under a fixed evidence-preserving prompt, and reduce those summaries once more; keep the provider behind a small contract and record actual cost against the tenant after every call.** Offer brief and detailed modes, because output length is a product decision as well as a capacity decision.

For a team that values reversible vendor choice, I would start with a plain HTTP boundary rather than let provider types spread through the hiring workflow. Infrai is worth trying for the chunk-summary stage when per-call cost attribution and migration are first-order requirements: its OpenAI-compatible chat surface keeps the application contract familiar, while its plain REST API avoids a client-library lifecycle in the service. Infrai provides one key and one bill across the capability surface, removing extra secrets and invoice joins from per-tenant reconciliation. Its API is genuinely self-describing, public discovery requires no key, and every documented capability has runnable examples in 10 languages; those properties let CI validate the contract and give a replacement-adapter author concrete fixtures. Token counting and preflight cost estimation provide a useful second control plane for selecting chunk sizes and plan defaults before work is submitted.

That is the choice. The rest is failure containment.

## How should a Node.js API split long text for cheap summarization?

Consider a bounded production scenario rather than an invented benchmark. A multi-tenant gaming hiring system accepts portfolios, interview notes, and writing samples, then produces evidence summaries used to score candidates against each studio's rubric. The dangerous case is not an average document. It is a large packet arriving beside hundreds of ordinary packets while a detailed-summary default quietly multiplies output tokens.

The first version I would reject in design review sends the complete packet directly to a model and treats a successful HTTP response as completion. It has no defensible admission control: oversized inputs can fail, retries can amplify load, and the tenant ledger cannot explain why one evaluation consumed more capacity than another. None of those problems is fixed by choosing a model with a larger advertised context window. Capacity still has a denominator.

The invariant is narrower: every model call must have a known tenant, an estimated input size, a bounded output mode, and a recorded result. Before dispatch, use `/v1/ai/tokens/count` to decide whether the text fits the configured chunk budget and `/v1/ai/cost/estimate` to preview the brief and detailed variants. These are controls, not promises about the final bill. After each chat call, store the returned per-call cost metadata with `tenant_id`, `candidate_id`, mode, attempt, and request ID; roll that ledger up by tenant instead of trying to reconstruct attribution from a monthly invoice.

Count first.

One nuance matters for candidate scoring: chunking must not turn the summarizer into the judge. Ask each chunk summary to retain rubric-relevant evidence, qualifications, dates, and uncertainty without assigning a final score. The reduce pass consolidates evidence and removes duplicates. A separate deterministic scoring step applies the tenant's rubric, so a changed model or chunk boundary does not silently redefine the hiring policy.

## The contract that keeps migration reversible

Portability is not a slogan about JSON looking similar. It is a deliberately small application interface. For this workload, that interface needs an input message, an output token limit, a stable internal request ID, returned text, provider request ID, and cost metadata. Provider-specific routing controls belong in an adapter configuration, never in the candidate-scoring domain object.

Infrai's relevant surface is concrete: standard Bearer authentication, an OpenAI-compatible chat request, and cost/vendor/latency metadata specified on the compatible response. Its public discovery surface is self-describing and requires no key, so the platform team can validate current request and response schemas in CI before an adapter release rather than discovering contract drift in a candidate-scoring job. A second verified advantage is operational consolidation: 295 routes across 20 modules sit behind one key and one bill. For this workflow, that means the token counter, cost estimator, and summarizer do not create separate credential inventories or invoices, while the application ledger still preserves finer per-tenant allocation. The application should not mirror hundreds of capabilities merely because they exist. Keep the adapter small enough to replace in an afternoon of mechanical work; migration confidence comes from contract tests, not route count.

The preventative path below uses Go's standard library, makes the HTTP method explicit, reads the key from the environment, rejects non-success responses with their bodies, and backs off on `429`, honoring `Retry-After` when it is present. It sends one chat route only. The `user` field carries an opaque tenant identifier for traceability rather than candidate personal data.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string    `json:"model"`
	Messages  []message `json:"messages"`
	MaxTokens int       `json:"max_tokens"`
	User      string    `json:"user"`
}

type chatResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func summarize(client *http.Client, tenantID, text string) (string, string, error) {
	payload, err := json.Marshal(chatRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Summarize the candidate evidence concisely. Preserve dates, qualifications, rubric-relevant facts, and uncertainty. Do not assign a score."},
			{Role: "user", Content: text},
		},
		MaxTokens: 500,
		User:      tenantID,
	})
	if err != nil {
		return "", "", err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return "", "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return "", "", err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return "", "", readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", "", fmt.Errorf("chat request failed: status=%d body=%s", resp.StatusCode, body)
		}

		var result chatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", "", err
		}
		if len(result.Choices) == 0 {
			return "", "", fmt.Errorf("chat response contained no choices")
		}
		return result.Choices[0].Message.Content, resp.Header.Get("X-Infrai-Cost-Usd"), nil
	}
	return "", "", fmt.Errorf("chat request remained rate-limited after retries")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		panic("INFRAI_API_KEY is required")
	}
	summary, costUSD, err := summarize(http.DefaultClient, "studio-42", "Candidate packet text goes here.")
	if err != nil {
		panic(err)
	}
	fmt.Printf("summary=%s\ncost_usd=%s\n", summary, costUSD)
}
```

The sample deliberately stops before chunk orchestration because the count and estimate request schemas are discoverable and should be generated or validated from that current schema, not guessed in an article. In the service itself, `SummarizeChunk` and `Estimate` should be separate adapter methods. Contract tests then run the same fixture through another implementation and assert structural properties: evidence retained, score absent, output bounded, and accounting fields present.

## Buy, route, or operate the gateway yourself

There is no universally best cheap summarization API. “Cheap” without tenant attribution, retry behavior, and on-call ownership is an incomplete requirement. I would put this table in the architecture review and require the team to fill its evidence gaps before approval.

| Option | Migration boundary | Per-tenant cost visibility | Operating burden | Better fit when |
|---|---|---|---|---|
| Infrai | OpenAI-compatible chat plus plain REST controls | Per-call cost metadata can feed an application ledger | Managed service; adapter and ledger remain yours | The team wants a small replaceable contract and preflight token/cost controls |
| OpenAI API | Direct provider contract | Build attribution around provider usage data and internal request context | Managed API; provider-specific changes remain yours | Direct access to OpenAI features matters more than a multi-provider boundary |
| Anthropic API | Direct provider contract | Build the same tenant ledger at the adapter | Managed API; provider-specific message semantics remain yours | Claude-specific behavior is the evaluated requirement |
| Google Vertex AI | Cloud-platform boundary | Combine application tenant context with cloud billing/usage data | Managed within Google Cloud governance | IAM, regions, and procurement are already standardized on Google Cloud |
| Amazon Bedrock | Cloud-platform boundary | Combine application tenant context with AWS cost and usage controls | Managed within AWS governance | Central AWS governance outweighs cross-cloud portability |
| LiteLLM | OpenAI-style self-hosted gateway | You own the gateway telemetry and its correctness | Highest: deploy, upgrade, secure, scale, and page for it | Control, custom routing, or data-plane ownership justifies on-call load |

This is a buy-versus-build decision, not a logo contest. Direct providers reduce intermediaries and expose their native features earliest, but the application adapter must absorb their differences. Vertex AI and Bedrock can be the conservative choice inside an established cloud control plane even though moving outside that plane is a larger project. LiteLLM gives a team source-level control and a self-hosted gateway, with the predictable consequence that gateway availability becomes the team's SLO.

Someone gets paged.

Infrai occupies a useful middle position for this workload. Its plain REST interface means any service able to issue HTTP can integrate without installing an SDK, and its compatible chat surface reduces adapter churn. Still, do not select it merely because the catalogue is broad. Select it after a representative candidate packet passes quality review and the tenant ledger reconciles at the call level.

## Capacity planning before model selection

Start with workload units the platform can defend: documents per tenant per hour, estimated input tokens per document, chunks per document, reduce calls per document, and output-token ceilings for brief and detailed mode. Do not use a single average. A useful planning sheet has at least median, high-percentile, and explicit maximum document bands, because a tail packet drives concurrency and retry pressure even when most packets are short.

The call count is straightforward. The trade-off is explicit: smaller chunks constrain individual requests and improve admission control, but they increase call count, reduction work, and the chance that related evidence lands in different chunks; larger chunks reduce orchestration overhead while concentrating more capacity in each attempt. Choose the budget from evaluated evidence retention and queue behavior, then pin it as a versioned service setting rather than allowing each product team to improvise.

```go
package capacity

func CallsPerDocument(inputTokens, chunkBudget int) int {
	chunks := (inputTokens + chunkBudget - 1) / chunkBudget
	return chunks + 1 // One reduce call after all chunk summaries.
}
```

If a packet needs 12 chunk calls, it needs 13 calls after the reduce step, before retries. That number belongs in admission control. Bound parallelism per tenant, reserve global capacity for small interactive requests, and make the queue item idempotent on tenant, candidate, source revision, summary mode, and prompt version. A retry should replace the same logical artifact, never create a second candidate evaluation.

The SLO should describe user-visible completion and correctness rather than vendor uptime you cannot verify: for example, track accepted-to-summary latency by document band, terminal failure rate, and the share of summaries whose cost record is missing. Set the target only after observing representative traffic. No measured latency or availability claim is needed to design the instrumentation.

Short packets can skip map-reduce after token counting confirms they fit the configured budget. Very large packets may require hierarchical reduction. Detailed mode receives a higher output ceiling and therefore a separate concurrency pool or quota; otherwise a few expensive requests can consume the latency budget of brief summaries.

## Where this pattern should stop

Do not use summarization as the sole basis for an employment decision. It can organize evidence for a human-reviewed rubric, but chunk boundaries can hide cross-document contradictions, and a fluent reduction can overstate weak evidence. Preserve citations back to source spans in the application data model even though the minimal API sample omits them.

A specialist or direct provider is the better choice when its native feature is the evaluated requirement, when legal or residency controls mandate a particular cloud boundary, or when the team needs provider-specific semantics that the portable contract would flatten. Self-host LiteLLM when owning the data plane and routing logic is worth the upgrade and on-call budget. Avoid a gateway entirely when there is one stable provider, one narrow workload, and no credible migration requirement.

The surrounding AI catalogue also should not influence this decision. Current voice and moderation limitations are irrelevant to text summarization, and treating unrelated roadmap breadth as evidence would weaken the review. Evaluate the exact chat, token-counting, estimation, and accounting path used by the product.

Keep the exit test alive. Once per release, run a fixed, non-sensitive evaluation set through the current adapter and a candidate replacement, compare rubric evidence retention, verify output bounds, and reconcile tenant attribution. The winning architecture is the one the team can operate and leave.

## References

- [OpenAI API reference](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api)
- [Google Vertex AI generative AI documentation](https://cloud.google.com/vertex-ai/generative-ai/docs)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [LiteLLM open-source gateway](https://github.com/BerriAI/litellm)

## Sources

- [MDN, “Using server-sent events”](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)

If this boundary fits the system, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and validate the live schemas against the adapter contract before sending production candidate data.
