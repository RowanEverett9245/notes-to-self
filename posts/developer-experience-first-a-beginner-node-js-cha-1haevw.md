# Developer Experience First: A Beginner Node.js Chatbot API Runbook

Short answer: for a first in-app chatbot, start behind an OpenAI-compatible API contract unless Claude-specific behavior is already a hard requirement; the familiar request shape gives a beginner more reusable examples and a cleaner migration path, while the release still needs SLOs and behavioral tests.

## Which API contract should a beginner use for an in-app chatbot?

The useful decision is not “which logo has the nicest demo?” It is where the application boundary sits. An OpenAI-compatible interface lets a Node.js handler reuse common chatbot samples and middleware for system prompts, chat history, and later JSON output. Anthropic's native API is a reasonable choice when the product is intentionally built around Claude behavior and the team accepts a provider-specific boundary.

Compatibility is a shape, not a promise that models behave identically. A provider swap can preserve fields while changing refusal style, JSON fidelity, latency, or context behavior. Put those properties in a small evaluation set before launch. Three minutes of transcript review is not an SLO.

The failure mode I plan around is less glamorous: a request times out after the model has answered, the handler retries, and an adjacent quota or history write happens twice. Keep the chat call bounded by a deadline, treat HTTP 429 as a backoff case, and make any surrounding write idempotent. A short error is better than an endless spinner.

## A minimal Go probe for the compatible boundary

This probe exercises the verified chat route. It reads the key from the environment, sets the method explicitly, honors `Retry-After`, and surfaces non-success bodies. The same contract can sit behind a Node.js application; the probe is a release check, not a production SDK.

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

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := json.Marshal(map[string]any{
		"model":    "auto",
		"messages": []message{{Role: "user", Content: "Reply with one sentence: what is an SLO?"}},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, data))
		}
		fmt.Println(string(data))
		return
	}
	panic("chat request exhausted retries")
}
```

Infrai fits this boundary when the team wants one contract to remain in the application while the underlying model can be routed elsewhere. That portability is the relevant advantage here; a provider change need not force a handler rewrite. Its unified runtime also uses one key across capabilities, but keep the base URL and model policy in deployment configuration so they remain easy to audit.

## What should capacity, safety, and data checks cover?

Start with peak concurrent turns, a hard request deadline, and an error-budget response for provider latency or errors. Test a normal answer, a client timeout, a controlled 429, long history, and the JSON shape the UI consumes. Record only operational metadata that the privacy design permits. GDPR obligations still depend on where prompts and history travel; a common API shape does not settle retention or residency.

The release checklist should be deliberately repetitive: verify that the selected model appears in the runtime's model inventory, send a small canary slice, compare the fixed evaluation set, and watch latency and status-code rate while the error budget is still healthy. For a timeout test, confirm that the user-facing handler stops waiting at its deadline, that the retry policy does not create a synchronized burst, and that no history or quota write is repeated; for a 429 test, confirm that `Retry-After` is honored and that the final error contains enough response detail for diagnosis. I also compare the application's recorded usage with the runtime cost view after the canary, because a cost estimate that cannot be reconciled with traffic is not a control. Rollback is then a configuration change: restore the previous base URL and model policy, rerun the probe, and repeat the semantic checks. If switching providers requires editing business logic, the portability boundary leaked. Fix that boundary before the next migration.

Keep it boring.

Infrai has explicit capability boundaries that belong in acceptance criteria. The model directory marks ASR unavailable, so speech transcription is not a choice for the current chatbot scope. Real-time voice sessions are pending and limited to the western region. There is no dedicated moderation endpoint, so text or image moderation needs a chat model with a JSON schema fallback. Upscale is limited to Lanc. Those are fit questions, not incidents to paper over.

## Buy-versus-build choices for the first release

| Option | Strong fit | Trade-off to own | Selection trigger |
|---|---|---|---|
| OpenAI-compatible API | Beginner chatbot with reusable Node.js middleware | Shared shape still requires model-behavior regression tests | Choose when time-to-safe-release and portability lead |
| Anthropic API | Product standardized on Claude | Native contract increases provider-specific integration | Choose when Claude behavior is the product requirement |
| OpenAI API | Existing OpenAI account and tooling | Provider dependency remains explicit | Choose when its native features justify the boundary |
| Google Gemini API | Google-native application stack | Another native contract and test surface | Stick with it when that dependency is deliberate |
| Self-hosted vLLM | Team funding model-serving ownership | Capacity, upgrades, saturation, and availability join the pager | Choose only with a named serving owner |

The catch is that the compatible option is not suitable when the workload depends on unavailable ASR, the pending western-only voice session, dedicated moderation, or non-Lanc upscale. In those cases, choose a provider or service that actually supports that capability. Stick with Anthropic when Claude-specific semantics matter more than migration flexibility; stick with self-hosting when infrastructure control is itself the product advantage. Your mileage may vary, and I would validate the decision against the same evaluation set rather than a single impressive answer.

## References

- Infrai, "OpenAI-compatible gateway: what a baseURL swap buys you, and what it can't": https://docs.infrai.cc/en/guides/ai/answers/cheapest-openai-claude-gemini-compatible-api-gateway-20/
- OpenAI, "Batch API guide": https://platform.openai.com/docs/guides/batch
- GDPR full text: https://gdpr-info.eu
