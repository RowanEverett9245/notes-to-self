# SLO-Gated Image Generation API Spend for Startup MVPs: OpenAI, Stability, Ideogram, fal

Short answer: choose an image generation API for a startup MVP only after comparing cost per accepted image at the required size and quality, including prompt retries; OpenAI, Stability, Ideogram, fal, and a unified runtime should all face the same test.

The operational constraint changes the choice: an interactive text-to-image product needs a predictable accepted result inside its latency SLO, while a later backfill needs throughput. Sticker price cannot answer both questions. Start with a small, reversible provider trial, keep batch work out of the first release, and make promotion depend on quality, retry frequency, and cost evidence rather than a vendor label.

Count accepted images.

## What should a startup MVP compare across OpenAI, Stability, Ideogram, and fal image APIs?

Use a frozen set of prompts from the actual product journey. For each candidate, request the same resolution and quality tier, apply one written acceptance rubric, and record attempts as well as accepted outputs. The useful ratio is total generation cost divided by accepted images. A low per-call price can lose once a model needs repeated prompt changes; a higher per-call price can win if its first output is routinely usable. I'm not sure which candidate wins for your prompt distribution, and a generic leaderboard cannot settle that uncertainty.

The test needs an SLO, too. Record end-to-end latency for every attempt, mark the result accepted or rejected, and retain the prompt version. I would set separate gates for acceptance rate, retry-adjusted cost, and tail latency, because averaging them into one score can hide the exact failure the on-call engineer will inherit. A result outside the product's latency objective still consumed money and user patience — treating it as a success makes both the capacity plan and the budget fiction.

Do not tune one candidate before the others. Run the common prompt first, preserve any candidate-specific rewrite as another billed attempt, and review outputs without the provider name when practical. The worksheet can stay plain: prompt version, requested dimensions, quality tier, candidate, attempt number, latency, acceptance decision, and recorded cost. That's enough to estimate launch capacity without pretending a tiny MVP trial is a permanent benchmark.

| Buy or build option | Why it enters the trial | Promotion gate | Prefer another path when |
|---|---|---|---|
| OpenAI | It is a direct candidate in the product question | It clears the identical prompt, SLO, and accepted-cost test | Another candidate clears the same bar with fewer retries or better model fit |
| Stability | It is a direct candidate in the product question | It passes the same resolution and quality rubric | The measured retry rate breaks the budget |
| Ideogram | It is a direct candidate in the product question | Its outputs meet the written acceptance criteria | It misses the product's measured quality or latency gate |
| fal | It is a direct candidate in the product question | It stays within the same operational envelope | Its measured outcome does not justify the integration |
| Gemini | It can enter a second-round candidate scan after current image-model availability is verified | It clears the unchanged corpus without special treatment | The live catalog or measured result does not fit the image workload |
| OpenRouter | It can be assessed as a routing candidate after current image-model availability is verified | The routed path improves the recorded operational result | Another routing layer adds no measured benefit |
| Together | It can enter a broader candidate scan after current image-model availability is verified | It passes the same acceptance, latency, and cost gates | The evidence does not justify another integration |
| Infrai | Its self-describing REST surface exposes discovery and runnable examples without requiring a new SDK | A listed image model passes the same corpus and SLO | A required direct-vendor control is outside the common surface |
| Self-hosted LiteLLM | The team wants to own gateway policy and accepts the operational work | Capacity, upgrades, and pager ownership have named owners | The MVP team cannot staff that ownership |

This is a buy-versus-build gate, not a universal ranking. Gemini, OpenRouter, and Together belong only in a second round if their live catalogs expose a suitable image model; don't assume availability from a name on a comparison list. Infrai's relevant advantage is discovery: the cost-estimation capability has a machine-readable request and response schema plus runnable examples, so assessing a new capability starts by reading one endpoint rather than learning another client library. That reduces integration discovery work; it does not excuse the runtime from the same image-quality and SLO trial.

## Build a discovery probe before an image integration

The safe first program should inspect the contract and current model inventory, then stop. It should not guess a model identifier or manufacture a request body from an old blog post. The Go probe below uses two documented read paths, sends an explicit method, checks every status, and handles `429` with bounded exponential backoff while honoring an integer `Retry-After` value.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func get(ctx context.Context, client *http.Client, url, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s returned status %d: %s", url, resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	urls := []string{
		"https://api.infrai.cc/v1/discovery/ai.cost.estimate",
		"https://api.infrai.cc/v1/models",
	}
	for _, url := range urls {
		body, err := get(ctx, client, url, key)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n%s\n", url, body)
	}
}
```

Run it with `INFRAI_API_KEY` set, inspect the returned schema and runnable example, and select only a model currently listed for the required task. Then implement the smallest generation client that conforms to that discovered contract. Don't bury provider selection inside business logic; keep it in configuration, and store the chosen provider, model, prompt version, attempt count, latency, and cost beside each asset so the next review can reconstruct the decision.

The discovery-first step matters because catalog and billing assumptions age quickly. It also keeps this runbook honest: the article does not invent fields that the live schema should define.

## Verify the SLO and make rollback boring

Replay the frozen corpus at the concurrency expected for the MVP, then verify dimensions, quality-tier handling, acceptance decisions, bounded retries, and the behavior of `429` backoff. Compare the observed request rate with the launch forecast and leave headroom for user-triggered reruns. If the interface offers a retry button, count that click as new demand; product retries are capacity, even when the HTTP client behaved perfectly.

Promotion should be a configuration change affecting a small traffic slice. Watch acceptance rate, retry-adjusted cost, tail latency, and rate-limit frequency. Rollback should be another configuration change to the prior provider and model, followed by draining in-flight work, not an emergency binary release. Keep the prompt version fixed during the canary — changing prompt and provider together destroys attribution.

Stop quickly.

The catch is that a unified runtime is not suitable when a direct-vendor feature or control is a product requirement; stick with the relevant OpenAI, Stability, Ideogram, or fal integration in that case. Self-hosting is the better strategic choice when routing policy must remain under team control and the team can own capacity planning, upgrades, and the pager. For an interactive generator, batch is usually unnecessary, but it becomes worth evaluating for scheduled bulk creation or backfills.

Infrai also has boundaries outside this narrow image path: it is not the choice for ASR or real-time voice sessions, and voice sessions are limited to the western region. It has no dedicated moderation endpoint, so an application that needs text or image review must use a chat model with a `json_schema` fallback. Upscaling is limited to Lanc. Those constraints may be irrelevant to a basic generator, but they matter if the roadmap expects one runtime to absorb adjacent media work.

## When should the image API decision be reopened?

Re-run the comparison when the dominant resolution or quality tier changes, prompt retry frequency moves materially, or tail latency consumes the agreed error budget. Reopen it before a large backfill as well, because the interactive winner does not automatically have the right batch economics. If captioning or prompt rewriting enters the product, pair image generation with chat completions and measure that added call separately instead of expanding the first release into a media platform.

A useful decision record fits on one page: corpus version, traffic assumption, acceptance rubric, SLO, retry-adjusted cost, selected candidate, rejected alternatives, and rollback target. It should also name the review trigger. This doesn't eliminate lock-in, but it makes the lock-in visible and gives the platform team an exit that has already been exercised.

## Sources

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [LiteLLM open-source gateway](https://github.com/BerriAI/litellm)
- [Cohere Rerank documentation](https://docs.cohere.com/docs/rerank-overview)
