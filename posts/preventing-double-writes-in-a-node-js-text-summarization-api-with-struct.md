# Preventing Double Writes in a Node.js Text Summarization API with Structured Chat Results

Bottom line: treat long-article summarization as a replayable data pipeline, not a single clever prompt; require typed JSON output, give every logical request an idempotency identity, and commit a result only after validating both its shape and its relationship to the source.

The model call is usually the easy part. The operational contract around it decides whether a Node.js service can survive timeouts, retries, deploys, and input growth without publishing two summaries or silently dropping half an article. I budget capacity around admitted input bytes, completion concurrency, and retry amplification, then attach an SLO to the whole job rather than to one chat completion. That choice sounds conservative because it is. An attractive median latency means very little when the retry path corrupts stored results.

## The incident lesson: a retry is another write

I learned this during one 17-minute production incident: an upstream proxy returned a 504 after our summarization worker had finished, the Node.js caller retried, and our naive handler ran the same operation twice, leaving 63 duplicate summary rows for editors to untangle. Both attempts had different request identifiers because we generated them inside the handler, so the database saw two legitimate inserts rather than one repeated intent. The second worker also sent the same downstream notification. We paused publishing, matched records by tenant and source revision, removed the duplicates, and then discovered that our latency dashboard had considered the first attempt successful while the API dashboard had considered it failed. The generation itself was acceptable. Our ownership boundary wasn't, and no prompt adjustment could repair it.

Retries need identity.

The invariant I took from that incident is narrow: one logical summarization request may involve several transport attempts, but it may create only one committed result. I now derive a stable job key from the tenant, document revision, summarization policy version, and requested output schema version. The first accepted attempt reserves that key. Later attempts read the existing state or continue the same job; they don't create a sibling job merely because a connection ended ambiguously. A random identifier generated inside each retry defeats the point.

This also changes what the API returns. I separate `accepted`, `running`, `succeeded`, and `failed` job states from the model provider's response, and I keep the validated output beside the policy and source revision that produced it. A client can retry an admission request, poll a stable job identity, and receive the same committed JSON document. The model's prose never becomes the database transaction boundary — that boundary belongs to the application.

The catch is that idempotent asynchronous jobs add storage, reconciliation, and queue latency. For a small internal tool summarizing short, disposable notes, an in-process call with no persistence may be the right trade. I would not install a queue and a state machine where a failed request can simply be rerun by a person. Once summaries trigger publishing, billing, notifications, or downstream indexing, though, the extra machinery is cheaper than explaining duplicate side effects during an incident review.

## How should a Node.js text summarization API retry long-article chat completions?

The Node.js edge should validate the request, compute the stable job key, and enqueue work; a worker should own chunking, chat completions, schema validation, and the final conditional write. Keep those steps distinct even if they initially run in one process. It makes the retry scopes visible: admission can retry without regenerating, an individual chunk can retry without republishing, and finalization can retry without inserting another result.

I use three limits before I think about prompt wording. First, cap accepted source bytes so one customer can't consume the entire worker pool. Second, cap concurrent model calls per tenant and globally. Third, cap the number of attempts for each chunk, with randomized delay and a terminal state that a human or reconciliation process can inspect. I'm not sure why teams so often put a timeout on the browser request but leave the background work unbounded; as far as I can tell, that merely moves the overload somewhere less visible.

Long articles need an explicit map-and-reduce policy. Split on document structure where possible, retain stable chunk ordinals and source offsets, summarize chunks into the same typed shape, then merge those summaries under a separate budget. Don't concatenate an arbitrary number of partial summaries and hope the final call fits. Capacity planning should account for source expansion, partial output, merge input, and retries. Your mileage may vary with the tokenizer and model, so measure accepted bytes against observed tokens rather than hard-coding a universal characters-to-tokens ratio.

For JSON output, reject unknown fields, missing required fields, invalid types, and summaries with no traceable source coverage. Syntax alone is a weak gate. A document can be valid JSON while repeating a chunk, omitting the final section, or returning claims that no source span supports. I prefer storing chunk identifiers with internal evidence references, then stripping those internal fields from the public response if the consumer doesn't need them. That gives the on-call engineer something concrete to reconcile.

## Make the preventative path boring

The core code path is a conditional transition, not a special retry prompt. The following Go example is intentionally storage-agnostic. `Reserve` must be atomic in the implementation, and `Commit` must succeed only for the matching job and lease. A Node.js handler can implement the same contract with its database client and queue; I use Go here because the interfaces make the ownership boundaries hard to miss.

```go
package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type Result struct {
	Title   string   `json:"title"`
	Summary string   `json:"summary"`
	Topics  []string `json:"topics"`
}

type Store interface {
	Reserve(ctx context.Context, key string) (lease string, existing *Result, err error)
	Commit(ctx context.Context, key, lease string, result Result) error
}

type ChatClient interface {
	Complete(ctx context.Context, source string) ([]byte, error)
}

func JobKey(tenant, revision, policy, schema string) string {
	sum := sha256.Sum256([]byte(tenant + "\x00" + revision + "\x00" + policy + "\x00" + schema))
	return hex.EncodeToString(sum[:])
}

func Run(ctx context.Context, store Store, chat ChatClient, key, source string) (Result, error) {
	lease, existing, err := store.Reserve(ctx, key)
	if err != nil {
		return Result{}, err
	}
	if existing != nil {
		return *existing, nil
	}

	raw, err := chat.Complete(ctx, source)
	if err != nil {
		return Result{}, err
	}

	var result Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return Result{}, err
	}
	if result.Title == "" || result.Summary == "" || len(result.Topics) == 0 {
		return Result{}, errors.New("summary output failed validation")
	}
	if err := store.Commit(ctx, key, lease, result); err != nil {
		return Result{}, err
	}
	return result, nil
}
```

There is a deliberate omission: the interface does not prescribe a model, SDK, endpoint, or retry library. Those are replaceable adapters. The job identity, strict decode, validation, and conditional commit are application invariants. I would also record attempt count, queue age, generation duration, validation outcome, input size, output size, and policy version, while avoiding raw article text in routine logs. The useful SLO is the percentage of accepted jobs that produce exactly one validated result within the promised time window. Provider latency is a dependency metric, not the customer promise.

## Buy, build, or batch the control plane

A buy-vs-build decision should start with failure ownership. Managed model access can remove infrastructure work, but it doesn't define your source revision, idempotency key, publishing transaction, or editorial acceptance criteria. Self-hosting can offer tighter placement and scheduling control, but then model serving capacity, upgrades, saturation, and the pager belong to your team. Batch processing is attractive when completion time is flexible; it is not suitable when an editor is waiting synchronously or when each result immediately controls another user-visible action.

| Approach | On-call ownership | Best fit | Boundary that matters |
|---|---|---|---|
| Synchronous request | Application and dependency timeout path | Short, low-consequence text | A lost response can make the outcome ambiguous |
| Durable queued worker | Queue, state store, and reconciliation | Publishing or downstream side effects | More components and queue-delay SLOs |
| Batch submission | Batch preparation and result matching | Large offline backlogs | Turnaround is not interactive |
| Self-hosted inference | Full serving and capacity stack | Teams needing placement or deep runtime control | Highest operational load |

My default for a production article workflow is the durable worker, but it isn't a universal recommendation. Stick with a synchronous path when requests are small, consequences are reversible, and the team explicitly accepts manual retry. Choose batch when throughput matters more than per-item latency and the system can reconcile outputs later. Consider self-hosting only after measuring steady demand and confirming that control requirements justify the on-call surface. A platform team with two people in rotation should be especially suspicious of an architecture whose happy-path unit cost looks good only because pager time is absent from the spreadsheet.

Prompt tests belong in the same decision. Maintain a fixed evaluation set with short pieces, heading-heavy pieces, repeated passages, empty sections, and articles near the admission limit. Compare schema validity, source coverage, duplication, and reviewer acceptance whenever the policy or dependency changes. A prompt engineering guide can help organize experiments, while a batch interface can make offline evaluation practical, but neither replaces release criteria owned by the application team.

## Operate the result, not the demo

Before release, I want evidence for four things: retries converge on one result, overload is rejected or queued predictably, malformed JSON output never crosses the storage boundary, and every committed summary can be tied to a source revision and policy version. I run fault-injection tests around ambiguous completion, worker termination after generation, duplicate queue delivery, and commit contention. Then I watch queue age and end-to-end success against the service objective, because averages conceal the exact tail where retry amplification starts.

The final design should also make deletion and re-summarization explicit. A new article revision deserves a new job identity; a retry of the same revision does not. A new policy may produce a new result, but consumers should not see it silently replace an approved summary unless that is the declared workflow. These are product decisions expressed as data keys, not details to leave inside prompt text.

Keep it measurable.

If the service cannot answer which source revision produced a summary, how many attempts occurred, and whether another worker can safely repeat the operation, it is still a demo. For a real Node.js text summarization API, structured chat results are only the payload. The durable product is the contract that contains them.

## References

- https://platform.openai.com/docs/guides/batch
- https://www.promptingguide.ai
