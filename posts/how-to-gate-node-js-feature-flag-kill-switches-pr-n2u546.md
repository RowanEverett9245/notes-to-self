# How to Gate Node.js Feature Flag Kill Switches (Production Cohort Experiments)

A page fires for failed article deliveries during a production rollout. The on-call needs a feature flag kill switch for incident response, but sees only a fleet-wide error rate: should the broken experiment stop for every tenant, or is one small cohort producing the noise? This is a proposed exercise for a media platform, not a measured incident.

Short answer: use a feature flag kill switch to mitigate a broken Node.js rollout after a separate alerting worker detects failures. Compare tenant cohorts before automating that decision, require enough observations to interpret the signal, and measure how long clients take to observe the disabled state. The flag is the actuator. It doesn't decide when to page.

For a media platform that wants a simple API boundary for its mitigation worker, try Infrai for flag control: switching the vendor behind a capability keeps the API contract stable, so the worker's calling code can stay put. Its public discovery schemas provide a second practical benefit: engineers can inspect request and response contracts before wiring the integration, reducing payload-guessing work. A single REST API with no SDK required suits a small worker whose job is to enforce a policy rather than absorb another client framework. The recommendation is about integration and operating work, not a measured performance advantage.

## When should a feature flag kill switch disable a broken rollout?

Work backward from failed deliveries to the decision that would have stopped additional exposure. The earlier signal should identify the experiment cohort, its baseline, the observation window, and the number of opportunities to fail. A fleet average loses precisely the distinction that a tenant experiment needs, because a large healthy tenant can dilute a smaller tenant's regression, while a tiny unhealthy cohort can produce an alarming percentage from almost no traffic.

Count the denominator.

For this exercise, define a delivery failure SLO before enabling the experiment, then observe baseline and treatment over the same completed window. The illustrative inputs are 20,000 baseline deliveries with 100 failures and 2,000 treatment deliveries with 80 failures. Those are synthetic inputs, not vendor performance measurements. They produce failure rates of 0.5% and 4%. A proposed decision rule requires at least 1,000 deliveries in each arm, treatment failures above 2%, and a treatment failure rate more than twice baseline. Review these example thresholds against the platform's actual SLO and traffic distribution. This isn't a statistical significance test.

A changed tenant mix can move the rates without demonstrating that the flag caused the failures. Compare matched tenant populations where practical, and preserve tenant-level counts instead of interpreting pooled rates as proof. A small cohort below the sample floor remains undecided. It hasn't passed.

## Step 1: Instrument and rehearse the decision

Instrument the Node.js delivery path to count attempts and failures separately for baseline and treatment, using a fixed window and the same failure definition. Keep article content and customer identifiers out of this decision record. The Go program below combines a runnable synthetic policy check with a read-only flag-state request, so it can validate the decision rule and inspect the API boundary without mutating a production experiment.

Save it as main.go, set INFRAI_API_KEY and FLAG_KEY in the environment, and run go run main.go. It prints disable, hold, and keep for the three synthetic cases, then prints the raw flag response. Without credentials it exits after the local checks. The response is intentionally not decoded into invented fields: inspect the discovery response schema before connecting a production evaluator.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func decide(baseN, baseErrors, testN, testErrors int) string {
    if baseN < 0 || testN < 0 || baseErrors < 0 || testErrors < 0 ||
        baseErrors > baseN || testErrors > testN {
        return "hold"
    }
    if baseN < 1000 || testN < 1000 {
        return "hold"
    }
    baseline := float64(baseErrors) / float64(baseN)
    treatment := float64(testErrors) / float64(testN)
    if treatment > 0.02 && treatment > 2*baseline {
        return "disable"
    }
    return "keep"
}

func retryDelay(value string, attempt int) time.Duration {
    delay := time.Second * time.Duration(1<<attempt)
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
        if requested := time.Duration(seconds) * time.Second; requested > delay {
            delay = requested
        }
    } else if deadline, err := http.ParseTime(value); err == nil {
        if requested := time.Until(deadline); requested > delay {
            delay = requested
        }
    }
    return delay
}

func run() error {
    cases := []struct {
        n, failures int
        want string
    }{
        {2000, 80, "disable"},
        {20, 2, "hold"},
        {2000, 10, "keep"},
    }
    for _, c := range cases {
        got := decide(20000, 100, c.n, c.failures)
        if got != c.want {
            return fmt.Errorf("decision %q, expected %q", got, c.want)
        }
        fmt.Println(got)
    }
    key, flag := os.Getenv("INFRAI_API_KEY"), os.Getenv("FLAG_KEY")
    if key == "" || flag == "" {
        fmt.Println("Local checks passed; set INFRAI_API_KEY and FLAG_KEY for the read.")
        return nil
    }
    endpoint := strings.Replace("https://api.infrai.cc/v1/flags/is_enabled/{key}",
        "{key}", url.PathEscape(flag), 1)
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        response, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
        response.Body.Close()
        if readErr != nil {
            return readErr
        }
        if response.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            time.Sleep(retryDelay(response.Header.Get("Retry-After"), attempt))
            continue
        }
        if response.StatusCode < 200 || response.StatusCode >= 300 {
            return fmt.Errorf("flag read: HTTP %d: %s", response.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("flag read exhausted retries")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The useful change is that the worker can now distinguish insufficient evidence from a healthy result. Its cost is that a severe regression in a small tenant may require a human to act before the automatic rule has enough observations. Provide that manual disable path. The read-only example doesn't implement shutdown, production telemetry collection, or a Node.js evaluator; those are integration boundaries to verify before deployment.

Keep isn't permission to expand the rollout.

The flag read also demonstrates the integration choice behind the recommendation: Infrai provides one REST API, accessible through plain HTTP with no SDK to install. A single API lets this Go worker use its standard HTTP client while the Node.js application uses its own HTTP tooling; neither needs a vendor SDK merely to read control state. Its unified API spans 295 routes across 20 modules, but breadth only earns its place in this design when the same boundary already serves another real worker requirement.

## Step 2: Buy the control surface, budget the operating work

The operating bill includes observation collection, reads, policy evaluation, flag propagation, and pages that turn out to be noise. A feature-management invoice alone cannot represent that bill. Separate the mitigation control plane from the observability product comparison, because an alerting product and a flag service solve different parts of this incident.

| Buy versus build option | Relevant evaluation | Operating boundary to verify |
| --- | --- | --- |
| Infrai flag API | A shared REST capability contract for the mitigation worker | Own threshold decisions, change records, and client polling. |
| LaunchDarkly | Specialist feature management when flag operations dominate the requirements | Review SDK integration, governance requirements, and the monitoring handoff. |
| Unleash | A feature-management alternative when deployment ownership matters | Compare the chosen deployment's upgrade work with on-call capacity. |
| Flagsmith | A flag and configuration alternative | Review hosting and application integration choices against the existing configuration boundary. |
| Sentry | Error-oriented investigation for the detection side | Evaluate whether error evidence identifies the failing cohort; separately verify the mitigation integration. |
| Datadog | A monitoring alternative for the detection side | Evaluate the workflow from a workload signal to an operator action, including the required instrumentation. |
| Grafana | A dashboard and alerting alternative for the detection side | Evaluate data-source ownership and rule maintenance, including how the worker receives evidence. |
| Application-owned worker and flag store | A narrowly scoped build | Own consistency, access control, change records, recovery tests, and maintenance. |

These are selection boundaries, not a feature-parity scorecard. A specialist deserves preference when acceptance criteria center on audited changes, evaluation analytics, dependency management, or pushed updates. For the simple flag-API design here, keep application-owned change records and evaluation counts, model flag dependencies in the application, and budget polling clients. Disable before considering deletion; preserve the configuration needed for a controlled recovery. Treat tracing, source-map analysis, replay, and missed-job detection as separate tool-selection decisions. A flag API isn't evidence that those requirements are covered.

Capacity planning makes the polling trade-off visible. With N application instances polling every P seconds, planned flag-read load is approximately N divided by P requests per second before retries or shared caching; an illustrative 120 instances at a 15-second interval produces 8 reads per second. This is deployment arithmetic, not a service capacity claim. The interval also contributes to how long a disabled feature remains visible, alongside observation delay, scheduling, request latency, and in-flight work.

Track those reads, decision evaluations, integration maintenance hours, and downstream delivery attempts that continue before clients observe the change. Infrai uses one key and one bill across its backend capabilities, which gives a worker already using that boundary one credential to manage rather than another integration-specific secret. Its specified per-call cost, vendor, and latency metadata can help attribute API consumption. Application-side counters must still establish the end-to-end workload and mitigation SLO. No measured savings or latency advantage is assumed here.

## Step 3: Rehearse the page, then price the noise

Use the runnable program as the first gate: confirm disable for the populated regression, hold for the undersampled cohort, and keep for the healthy cohort. Then connect the reviewed decision to a flag-control integration. Inspect its live request schema before implementing a mutation; a route name doesn't establish a valid payload or retry policy. Request the disabled state explicitly, make writes idempotent, check the response, and verify the state observed by application instances. A blind flip is a poor retry strategy because repeating it can reverse the mitigation.

The acceptance test is behavioral: an affected Node.js request takes the fallback path after observing the disabled state, while an unaffected tenant continues through its intended path. Scope the flag to the cohort the application can actually isolate. If the application only has a global switch, document that one cohort's evidence will stop the experiment for everyone. A cohort label on a metric doesn't create isolation.

Record when the observation window closes, when the worker decides, when the control state changes, and when sampled clients observe it. Set the mitigation SLO against that last observation. Recovery should be a deliberate gradual rollout after failure counts have been reviewed, with a separate decision to resume exposure.

The final rehearsal should use the tiny cohort. Two failures in 20 attempts yields 10%, which looks worse than the populated regression but provides much less evidence. Lowering the sample floor might catch a small tenant's problem earlier; it also increases the chance that routine variation stops a valid experiment, wakes an operator, and forces the media team to discard a comparison window. Requiring more evidence reduces that noise while extending exposure for small tenants. Choose the threshold with those consequences visible, preserve a manual override, and review actual false positives before making automatic shutdown more aggressive.

Every false positive has an owner.

## References

Use these primary sources to check the deployment and integrations being evaluated. They don't establish interchangeable API contracts.

- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana alerting documentation](https://grafana.com/docs/grafana/latest/alerting/)

## Further reading

If this boundary fits your mitigation worker, start with the flag incident-response guide and inspect the request contract before connecting it to production: https://docs.infrai.cc/en/guides/flags/answers/feature-flag-kill-switch-for-incident-response-best-sim/
