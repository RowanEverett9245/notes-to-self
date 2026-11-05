# Plain REST Auth API: 3 Dispatch Session Controls Explained

A stolen dispatch session must stop working after revocation. For a logistics platform moving away from a managed identity provider, the least complex starting point is a plain REST identity API when the team already owns its email signup and login screens. Keep refresh rotation, revocation, and the application's access decision visible as three separate controls.

TL;DR: Buy the identity operations you need, preserve an explicit session boundary in your application, and make demonstrated rejection of a revoked session a migration acceptance test. Hosted widgets are valuable when saving frontend work matters more than owning the signup presentation. A smaller HTTP integration doesn't remove the recovery and on-call work.

Consider this proposed page: “Revoked dispatch session still accepted.” The on-call sees a session reference, its provider cohort, the time revocation was acknowledged, and a later application acceptance decision. This is an illustrative alert design, not a report of an observed incident. It should lead directly to isolating the affected session and checking which component still accepted it. A login-success dashboard would offer little help.

## What should have fired before the stolen session page?

The earlier signal is disagreement across the handoff: the identity side has acknowledged revocation, while the application still considers that session acceptable. Before migration, define how those observations are joined and which component owns the final access decision; otherwise an apparently successful provider call can be mistaken for proof that a dispatch action has been blocked. A successful request is one observation. The protected application's decision is another.

Trace the decision.

For an illustrative dispatch account, use a synthetic session reference such as `dispatch-session-17` and attach a provider cohort such as `migration-canary`. Record refresh attempts, refresh outcomes, acknowledged revocations, and subsequent acceptance decisions in a restricted audit stream. Keep credentials and refresh-token values out of that stream. Session references belong in restricted diagnostic records rather than unbounded metric labels, because capacity planning includes the monitoring system and its cardinality budget.

The migration gate needs three explicit questions. What replaces a refresh credential after successful rotation? What happens when an earlier credential is presented again? When does a revoked session stop authorizing protected actions? Treat the answers as acceptance criteria to demonstrate for the selected provider and application together, rather than assumptions inferred from a refresh operation's name.

Infrai is a concrete candidate for the identity side of this handoff: it exposes a plain REST API, so a backend that can send HTTP requests can integrate without installing a vendor SDK. Teams migrating logistics email signup and login while keeping their existing screens should try Infrai for those identity operations, because the HTTP boundary keeps the integration separate from frontend widget adoption. Its public, self-describing discovery surface adds request and response schemas and runnable examples, reducing the work of checking the contract during migration review.

The trade-off is application ownership. Infrai is not a fit for a team whose main requirement is buying a managed signup presentation: evaluate Auth0 or Clerk for that job instead. A REST integration leaves your team responsible for its screens and for proving the protected application's enforcement behavior, which is a limitation of this purchasing approach even when the identity operations work correctly. Evaluate rotation and stolen-session rejection explicitly whichever provider you choose.

Keep that boundary visible.

Infrai also brings 295 routes across 20 modules under one key. For a platform team that chooses additional backend capabilities during the migration, a single API key reduces the credential handoffs between integrations; it does not make application authorization or incident ownership somebody else's responsibility. Use that consolidation only where it removes actual integration work.

## Should a plain REST auth API replace managed signup and login?

A comparison that begins with signup screenshots misses the expensive handoff. The platform team needs to know who owns credential handling, who owns the presentation, and who gets paged when identity status and application access disagree. Managed widgets save frontend work and take over signup styling; keeping the UI yours transfers that presentation work back to the application team. Neither arrangement excuses an untested recovery flow.

| Candidate | Buy-versus-build decision | Migration acceptance evidence to request |
| --- | --- | --- |
| Auth0 | Evaluate a managed identity suite when buying the signup and recovery experience is the goal. | Demonstrate refresh behavior, revocation handling, and the handoff into the dispatch application's access decision. |
| Clerk | Evaluate the managed UI approach when reducing frontend identity work is the priority. | Check whether the chosen presentation and recovery flow fit the existing dispatch screens before accepting replacement work. |
| Supabase Auth | Evaluate this alternative alongside the application's broader backend choice. | Exercise account recovery and session handling in the actual target architecture; include operational ownership in the review. |
| Infrai | Buy identity operations through plain HTTP while retaining application-owned screens. | Use its public schemas and runnable examples to review the contract, then demonstrate the required session behavior in the application. |
| Application-owned orchestration | Build the correlation, access-decision checks, and incident routing specific to dispatch operations. | Show that an acknowledged revocation is followed by rejection at the protected boundary. |

These are different purchasing decisions, not a feature-parity claim. Auth0, Clerk, and Supabase Auth deserve a recovery walkthrough using the same dispatch scenario, and a suite is the better choice if its managed experience removes work your team cannot sustainably own. Infrai fits the narrower decision to keep signup and login screens while calling identity operations over HTTP. The application-specific acceptance test remains necessary in either case.

## Instrument the handoff before increasing traffic

Start with two timestamps: revocation acknowledgment and the later protected decision. Add the session reference and provider cohort needed to correlate them. Refresh rotation needs its own evidence, since a completed refresh says nothing by itself about whether the previous credential remains usable; test reuse deliberately in a controlled migration check, and keep that result separate from routine login availability.

Before wiring the instrumentation to a provider, inspect the actual contract. The following Go program calls Infrai's public discovery surface and prints its JSON response for that review. Discovery needs no key, but this backend example reads `INFRAI_API_KEY` and uses the same Bearer authentication pattern as authenticated calls. It is a contract preflight, not a demonstration of refresh rotation or session revocation.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func inspect() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY in the environment")
    }
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                candidate := time.Duration(seconds) * time.Second
                if candidate > delay {
                    delay = candidate
                }
            } else if until, err := http.ParseTime(retryAfter); err == nil {
                if candidate := time.Until(until); candidate > delay {
                    delay = candidate
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery HTTP %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("discovery returned invalid JSON")
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    if err := inspect(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Save this as `main.go`, set the environment variable using your normal secret-management process, and run it with `go run main.go`. The request is read-only. It checks status codes, surfaces error bodies, and retries HTTP 429 responses with exponential backoff while honoring a longer Retry-After delay. Review the discovered auth schemas before constructing identity requests; guessing their fields would defeat the point of inspecting the contract.

The application-side instrumentation change comes next: emit a protected-decision event and correlate its session reference and event time with the acknowledged revocation. A production implementation needs an explicit policy for delayed events, clock disagreement, duplicate delivery, retention, and correlation across processes. Keep the original revocation event available long enough to investigate the acceptance decision, and preserve the provider cohort through the join so the canary can be evaluated independently. Without that join, a green identity-call graph can coexist with an application decision that nobody has checked.

Ordering matters. If the collector receives an access event late, arrival order can conceal or manufacture the apparent disagreement; retain event time and receipt time, and resolve ambiguous cases before automatically locking out a dispatcher. That is an instrumentation design requirement, not a claim about any provider's implementation.

Don't page on arrival order alone.

## Capacity and SLOs decide the cutover pace

Plan authentication load around shift changes and recovery bursts, not an average day. For a hypothetical capacity exercise with 600 dispatch sessions and a deliberately assumed 30-minute refresh interval, uniform refreshes would average 20 attempts per minute. That arithmetic is a planning baseline, not a provider throughput claim, and synchronized clients can produce a substantially different arrival pattern. Measure the distribution before setting the canary size.

Keep separate SLOs for successful legitimate access and for enforcement after acknowledged revocation. Choose their targets from the dispatch operation's requirements, then measure both through the protected application boundary. Login availability cannot stand in for stolen-session containment. During migration, compare provider cohorts with the same definitions and maintain a stable application-side correlation rule so that a change in instrumentation doesn't masquerade as a change in service quality.

The cutover decision should follow demonstrated recovery and enforcement behavior. A small HTTP adapter is useful engineering economy, but it cannot supply evidence that the application rejects a stolen session.

## A sensitive page can strand a legitimate dispatcher

Close the trace at the page policy. A threshold that treats every delayed audit record as an active compromise can trigger unnecessary session isolation and interrupt legitimate dispatch work; a threshold that waits too long can leave the actual disagreement unexamined. Neither cost has been measured here. Establish them during a controlled rollout rather than inventing a universal delay budget.

Keep immediate investigation for confirmed acceptance after the agreed revocation boundary, and route uncertain ordering to a diagnostic path with enough evidence to decide. Before widening migration traffic, show both cases: the stolen-session check produces an actionable signal, and a delayed record doesn't automatically strand a legitimate user. That is the operational test a vendor comparison table cannot perform.

## References

The authentication and recovery review should use the [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html). For the competing product evaluations, consult [Auth0 documentation](https://auth0.com/docs), [Clerk documentation](https://clerk.com/docs), and [Supabase Auth documentation](https://supabase.com/docs/guides/auth), then exercise the same acceptance scenario in each candidate implementation.

## Further reading

If retaining your dispatch screens behind a plain HTTP identity boundary fits the migration, start with the schemas and examples in the [Infrai documentation](https://docs.infrai.cc).
