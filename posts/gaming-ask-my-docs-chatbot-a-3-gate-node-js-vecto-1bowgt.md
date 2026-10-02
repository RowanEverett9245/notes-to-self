# Gaming Ask-My-Docs Chatbot: A 3-Gate Node.js Vector Database API

TL;DR: For an ask-my-docs chatbot over gaming product content, the simplest vector API is the one that lets a small team enforce three gates before indexing: stable document identity, content-change detection, and a capacity limit. Choose an operated service if avoiding infrastructure is a hard constraint, but make the decision from the write path backward. Query syntax is easy to wrap; uncontrolled re-embedding, duplicate chunks, and stale editions are the costs that compound.

The operating target should be explicit: retrieval has an availability and freshness SLO, while ingestion has a budget. Those are separate control loops. A client can retry a failed query without growing the corpus, but a carelessly retried indexing job can create more vectors every time it runs.

## What does an ask-my-docs vector database API incident reveal?

Consider a bounded production scenario rather than a claimed case study. A game publisher has store descriptions, downloadable-content pages, patch notes, and regional editions feeding one chatbot. A release pipeline receives the same content twice after a retry. If each delivery creates new chunks with random identifiers, the second run doubles those records; retrieval may then return near-identical passages, citations become less useful, and index consumption rises even though the playable catalog did not change. The exact growth depends on the chunker and embedding representation, so it must be measured rather than guessed.

Retries lie.

My capacity-planning rule is blunt: a retry must not increase the steady-state record count. That is the invariant. It applies regardless of which hosted API stores the vectors, and it matters more than saving a few lines in the first Node.js integration.

The preventative design assigns a deterministic identity from tenant, locale, source document, source version, and chunk ordinal. It also stores a digest of the normalized chunk text. An unchanged digest skips embedding and upsert; a changed digest replaces the same logical record; a deleted source version is reconciled deliberately. This makes index growth a function of live content, not delivery attempts.

The nasty version of this failure is not one obvious duplicate batch. A patch-note edit produces a new random ID; the old edition remains searchable; a second worker retries the edit; and all three passages now compete in the same nearest-neighbor result. The generator receives mutually stale context while the storage graph still looks healthy, because every individual write succeeded. An uptime chart will miss that failure. Inventory grouped by logical source version, plus a reconciliation pass against the publishing system, exposes it.

A useful planning equation is `live vector records = sum(chunks in each currently searchable document edition)`.

Do not substitute request count for that inventory. The write-side dashboard should show live records by tenant and locale, accepted and skipped chunks, deletion lag, and the age of the newest searchable release. The query dashboard should separately show latency, error rate, empty-result rate, and citation coverage. Mixing the two hides whether an incident is a serving failure or a corpus-control failure.

## Three gates before any vector write

The first gate is identity. Product slug alone is insufficient when the same game page exists in several locales or editions; conversely, a deployment timestamp is too volatile because it manufactures a new identity on every release. The key must represent the unit that search is allowed to replace.

The second gate is change detection. Normalize only transformations that are stable and intentional, then hash the result. If boilerplate removal or whitespace policy changes, version that transformation because it can alter every digest at once. Treat such a rollout as a capacity event, with a bounded batch and a rollback plan, rather than an innocent parser edit.

The third gate is admission control. Before scheduling embeddings, compute the proposed live-record delta against a per-catalog ceiling. A ceiling is not a universal magic number; it is a local safety setting derived from the publisher's catalog inventory, expected chunk distribution, headroom for a release, and the tolerated indexing bill. Rejecting or pausing an anomalous batch is preferable to discovering it from a monthly invoice.

The following Go example is intentionally on the ingestion side even if the chatbot application uses Node.js. It defines the service-neutral contract that the Node.js worker must call or reproduce, and it makes retries idempotent without depending on a vendor-specific SDK.

```go
package indexing

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
)

type Chunk struct {
    Tenant, Locale, DocumentID, Version string
    Ordinal                            int
    Text                               string
}

type Record struct {
    ID, Digest string
    Text       string
}

type Store interface {
    Digest(ctx context.Context, id string) (string, bool, error)
    LiveCount(ctx context.Context, tenant string) (int64, error)
    Upsert(ctx context.Context, record Record) error
}

func Prepare(c Chunk) Record {
    identity := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d",
        c.Tenant, c.Locale, c.DocumentID, c.Version, c.Ordinal)
    idSum := sha256.Sum256([]byte(identity))
    textSum := sha256.Sum256([]byte(c.Text))
    return Record{
        ID:     hex.EncodeToString(idSum[:]),
        Digest: hex.EncodeToString(textSum[:]),
        Text:   c.Text,
    }
}

func Admit(ctx context.Context, store Store, c Chunk, ceiling int64) (Record, bool, error) {
    record := Prepare(c)
    oldDigest, found, err := store.Digest(ctx, record.ID)
    if err != nil {
        return Record{}, false, err
    }
    if found && oldDigest == record.Digest {
        return record, false, nil
    }
    if !found {
        live, err := store.LiveCount(ctx, c.Tenant)
        if err != nil {
            return Record{}, false, err
        }
        if live >= ceiling {
            return Record{}, false, errors.New("catalog index ceiling reached")
        }
    }
    return record, true, nil
}
```

The sample omits vector persistence because an honest interface should not pretend every database accepts the same vector type, metadata expression, or consistency option. In the real path, call the embedder only after `Admit` returns `true`, attach the returned vector to the record, and then upsert by ID. Record the decision and outcome as bounded-cardinality metrics; raw document IDs belong in logs or traces with appropriate access controls, not metric labels.

There is still a race between the count check and the upsert. If several workers can add records concurrently, enforce the ceiling through a serialized tenant quota, a transactional ledger, or conservative batch reservations. The right mechanism depends on the chosen store. Pretending a read-then-write check is a hard distributed quota would be unsafe.

## Compare operating models, not demo ergonomics

A buy-versus-build review should expose who owns failure recovery and capacity, because "no infrastructure to run" is an operating requirement, not a preference for a short quickstart.

| Operating model | Team operates | Primary index-cost risk | Lock-in surface | Sensible when |
|---|---|---|---|---|
| Operated vector API | Ingestion, schema, evaluation, quotas | Unbounded records or writes hidden behind automatic retries | Filters, consistency semantics, export shape | The team cannot staff database upgrades, backups, and serving on-call |
| Managed relational database with vector capability | Schema, queries, tuning boundaries | Search workload competing with transactional or content workloads | SQL extensions and provider operations | Existing database ownership and measured workload isolation are acceptable |
| Self-hosted vector service | Full data and control plane | Reserved capacity, replicas, rebuilds, and idle headroom | Lower service dependency, higher operational coupling | Search is important enough to justify dedicated on-call and capacity engineering |

None is the universal winner. For the stated constraint, an operated API is the consistent category choice, but the shortlist still needs proof that deterministic upsert, metadata filtering, deletion, export, usage visibility, and quota controls behave as required. Keep product names out of the architecture decision until those tests exist. A polished SDK does not settle an on-call boundary.

The limitation is loss of control over maintenance timing, low-level tuning, and some data-plane behavior. An operated API is not suitable when the team must control those details, keep the service inside a constrained environment, or run predictable heavy workloads on already-owned capacity; a self-hosted service may fit instead, but only if that team accepts upgrades, backups, rebuilds, and serving incidents. A managed relational option is the middle trade-off when the organization already operates that database and can isolate search load. This is a staffing decision with a technical interface attached.

No shortcut there.

I would require a thin internal adapter around four capabilities: upsert by caller-supplied ID, filtered nearest-neighbor query, deletion by known scope, and inventory or export for reconciliation. The adapter is not an attempt to erase every semantic difference. It keeps application code stable while forcing those differences into configuration, conformance tests, and an explicit migration plan.

## Prove the retrieval path before scaling the corpus

Retrieval-augmented generation combines a generator with retrieved external passages; the original RAG work describes this as combining parametric and non-parametric memory. That architecture does not guarantee that a retrieved passage is current, authorized, or even relevant to the player's edition. The application owns those constraints.

Start with a representative slice of the gaming catalog, including localized pages, superseded patch notes, similarly named titles, and products with downloadable content. For each evaluation question, record the acceptable source documents and the required locale or edition filter. Evaluate retrieval before generation, since a fluent answer can conceal a poor candidate set. Then evaluate whether the final answer cites the retrieved product content and declines when the evidence is absent.

Use release-shaped load, not a flat request loop. Product updates arrive in bursts, and ingestion can contend with player queries even when the service hides its machines. Define separate objectives: a query latency and availability SLO, an indexing freshness SLO, and a hard capacity alert before the admission ceiling. Test retry storms, partial batch failures, delayed deletions, and restoration from the system of record.

Small corpus first.

Only after relevance and filters pass should the team project the full index. Count chunks from real documents under the chosen segmentation policy, group them by locale and content type, and add explicit release headroom. Re-run that inventory when the chunker, embedding representation, metadata replication, or retention policy changes. Price can be applied to the measured units during procurement, but it should not dictate the retrieval contract because commercial units change more readily than document identity does.

## Where this advice stops applying

The three-gate write path is unnecessary for a throwaway prototype whose corpus can be rebuilt from scratch and whose growth is intentionally bounded. It is also incomplete for regulated or player-specific documents: authorization must be enforced before retrieval results cross a trust boundary, and deletion requirements may demand auditable workflows beyond record replacement.

For a gaming catalog chatbot with no database on-call capacity, pick the operating model that removes database operations, then demand idempotent writes, scoped filters, reconciliation, observable usage, and a tested exit path. The decision is credible when a release retry leaves record count unchanged and a stale edition can be removed predictably. Everything else is demo polish.

## Sources

- https://arxiv.org/abs/2005.11401
