# Contract-First Text Classification Tagging JSON API Accuracy for Media

**Short answer:** test OpenAI, Claude, and Gemini as text classification and tagging backends only after the same JSON contract survives malformed requests, ambiguous labels, and retrieval gaps.

For a media team answering questions over a private knowledge base, the useful artifact is a bounded response that can be parsed, audited, retried, and safely shown to a reader. A model score from a friendly demo is not that artifact.

The answer is a contract.

Do not skip this test.

That sounds narrower than a model comparison, but it changes the operating decision. Suppose retrieval returns three source IDs and the generated answer names a fourth, plausible-looking document. A JSON parser will accept the object. A provider dashboard may report success. The archive workflow still has a false citation. The contract boundary is where that result becomes a rejected response, a measurable SLO event, and a controlled retry or review task. This is why schema checks, evidence checks, and refusal semantics belong in the application rather than in a prompt alone.

Treat the model as an untrusted dependency. Put a schema and a decision policy in front of it, measure contract violations as an SLO signal, and keep each provider behind the same small interface. This turns an OpenAI, Claude, or Gemini comparison into a replaceable experiment instead of an architectural commitment.

## What does a correct answer look like?

Start with the answer shape. A label without evidence is weak in a newsroom or archive workflow. A practical response can contain a bounded answer, source identifiers, a confidence band, and an explicit refusal state. `confidence` is not a calibrated probability unless it has been calibrated against the team's labeled set.

Reject unknown labels, extra keys, empty citations, and claims with no retrieved source. Distinguish an invalid model response from an unanswered question: the first calls for a retry or provider investigation, while the second may be the correct result.

A useful first pass is a 6-label taxonomy: `archive`, `rights`, `production`, `audience`, `history`, and `unknown`. These labels are examples, not a universal standard. Freeze their definitions, add adversarial examples, and version the schema when a label changes meaning. A schema version in every stored result prevents yesterday's annotations from silently acquiring today's interpretation.

## The failure signal is a contract breach

The important failure is often small: trailing prose after valid JSON, a citation ID that retrieval never returned, or a valid label paired with an empty answer. A parser catches the first problem. Application validation catches the other two.

Track three separate rates for the SLO: syntactically valid responses, semantically valid responses, and grounded responses. A 99.5% parse rate can still hide an unacceptable grounded-answer rate. Set the target with workflow risk in mind, then reserve an error budget for retries, manual review, and taxonomy changes.

Capacity planning is equally concrete. If peak traffic is 40 requests per second and one retry is allowed, provision for the peak plus the retry path, not the average. Bound input tokens, output tokens, and retry count. A provider timeout must not turn one request into an unbounded fan-out that consumes concurrency and delays archive jobs.

## A small Go boundary keeps the test honest

The provider adapter should return bytes and transport metadata; the domain layer owns validation. The same corpus can then exercise several providers without changing acceptance rules. This validator checks citations against retrieved evidence rather than trusting model-produced references.

```go
package answer

import ("encoding/json"; "errors"; "fmt")

type Result struct {
	SchemaVersion string `json:"schema_version"`
	Label string `json:"label"`
	Answer string `json:"answer"`
	SourceIDs []string `json:"source_ids"`
}

var allowed = map[string]bool{"archive": true, "rights": true, "production": true, "audience": true, "history": true, "unknown": true}

func Validate(raw []byte, retrieved map[string]bool) (Result, error) {
	var r Result
	if err := json.Unmarshal(raw, &r); err != nil { return Result{}, fmt.Errorf("invalid JSON: %w", err) }
	if r.SchemaVersion != "media-answer.v1" || !allowed[r.Label] { return Result{}, errors.New("unsupported schema or label") }
	if r.Label != "unknown" && r.Answer == "" { return Result{}, errors.New("answer is required") }
	for _, id := range r.SourceIDs { if !retrieved[id] { return Result{}, fmt.Errorf("source %q was not retrieved", id) } }
	return r, nil
}
```

Use a JSON Schema validator as the contract grows; `encoding/json` alone does not reject every unknown field or enforce every cardinality rule. Log schema version, provider class, latency, retry count, and validation outcome. Do not log private document text by default. Source IDs and a redacted request hash are more useful for incident review than another copy of the article.

## What makes a JSON API safe for text classification tagging?

Run a fixed, versioned test set instead of selecting a winner from a handful of prompts. Include factual questions, multi-document questions, ambiguous titles, missing evidence, empty retrieval, and labels that are close in meaning. Keep separate fixtures for European and US traffic when routing, language, or retention policy differs; geography is not an accuracy proxy.

A fair harness sends the same normalized evidence and contract to each candidate. Record parse validity, schema validity, citation validity, abstention quality, latency percentiles, retry frequency, and operator review time. Cost belongs in the capacity model, but it does not replace correctness: an inexpensive wrong label creates downstream work that a request price does not show.

| Candidate | Useful comparison boundary | Main limitation to test |
| --- | --- | --- |
| OpenAI | Structured response behavior and retrieval-adjacent classification | Validate the exact model version and schema behavior |
| Claude | Long-context question handling and refusal behavior | Test output parsing and evidence grounding on the chosen route |
| Gemini | Classification across the same evidence fixtures | Verify regional routing, limits, and contract enforcement |

The table is a test plan, not a ranking. Each candidate can be the wrong choice when its measured contract rate, latency, or operational controls miss the workload's SLO.

The three commonly evaluated families can differ around structured output, context limits, regional availability, and policy controls. Those properties change and must be verified from current primary documentation. Do not infer behavior from a model name or treat a passing prompt as an interface guarantee. Record the tested model version, schema, corpus revision, thresholds, and rollback owner.

Rollback is also a data operation. Stop writes from a failing adapter, route new requests to the last accepted adapter, and quarantine results created under the suspect schema version. Re-run validation before release. If the taxonomy changed, provider rollback cannot repair the semantic mismatch; restore the prior schema and migration rule too.

The durable choice is the system with evidence that it meets the contract under the actual workload, not a universal ranking. A private media knowledge base earns reliability through explicit refusal, bounded retries, source validation, and reversible annotations. The API is one component in that control loop.

## References

- RFC 8259, The JavaScript Object Notation (JSON) Data Interchange Format: https://www.rfc-editor.org/rfc/rfc8259
- JSON Schema specification: https://json-schema.org/specification
- OpenAI Embeddings guide: https://platform.openai.com/docs/guides/embeddings
- openai/whisper, open-source speech recognition: https://github.com/openai/whisper
