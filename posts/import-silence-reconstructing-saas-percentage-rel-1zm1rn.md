# Import Silence — Reconstructing SaaS Percentage Releases with Regional Feature Flags

Use percentage flags to stage the new importer, and make a missed result deadline independently observable before admitting customer tenants. A developer tools service can complete a scheduled import without producing usable results, so a record that the worker started cannot establish that its release succeeded.

TL;DR: Start with the flag off, test internally, then advance separate US and EU cohorts only while completed results meet the import freshness SLO. Preserve the evaluated flag decision with each run, including the configuration revision and tenant cohort, so an incident can be reconstructed after the rollout settings change. Use a dedicated dead man's switch for missing scheduled completions; percentage release control alone cannot detect silence.

The operational question is concrete: when a repository import stops producing results, can the on-call engineer distinguish a missed schedule, an intentional exclusion, and an admitted run that failed to deliver? A Node.js backend needs that evidence regardless of the language used to implement its flag evaluator. The Go example below isolates the admission decision so its behavior is inspectable without assuming a vendor's request schema.

## How should SaaS feature flag percentage rollouts preserve evidence of missing imports?

Start with an expected run, not a log search. Give each scheduled import a run ID and a result deadline, then record admission, start, and completion against that identity. Keep the source region and the tenant's cohort alongside those records. These are proposed application fields, not a monitoring service's API parameters.

Silence needs context.

An excluded tenant should still use the established importer; turning off the candidate must not turn off the job. If both implementations emit the same completion evidence, the operator can assess result freshness across the whole service while comparing candidate and control runs, rather than mistaking a reduction in candidate traffic for a disappearance of scheduled work. An empty upstream result may be valid. Define completion as a successfully committed result manifest, including an explicit empty result where appropriate, rather than requiring a positive item count on every execution.

For a proposed acceptance policy, suppose imports run every 30 minutes and must publish a result manifest within 10 minutes of their scheduled start. Those numbers describe a design example, not measured service behavior. The freshness SLI is the fraction of expected imports whose manifests meet that deadline; an illustrative 99.9% SLO gives the rollout a decision criterion, although the team still needs to choose a measurement window and exclusions appropriate to its own workload.

Keep the denominator.

Counting successful executions alone makes a scheduler that launches nothing look deceptively quiet.

## Admit stable cohorts and preserve the decision

Begin at 0%, enable an internal cohort, and consider customer stages such as 1%, 5%, and 25% only after observing the relevant deadlines.

Those are suggested stages.

They are not defaults promised by a flag service, and a small tenant population may make them too coarse to be useful.

Use separate flag keys for US, EU, tenant tiers, or beta groups when coarse targeting is sufficient. Region here means an application cohort; it establishes neither data residency nor a storage location. Keep configuration revisions in your own release records, and attach the revision actually evaluated to the run. Looking up the current percentage during an incident cannot recover what a worker saw earlier.

This runnable Go program demonstrates a local admission policy for a candidate importer. Its hash input deliberately excludes the run ID, so retries and later scheduled executions for the same tenant stay in the same bucket while the key remains stable. The revision is evidence, not a salt: changing a percentage should expand the admitted population without reshuffling it. This is an application-owned algorithm; it does not reproduce or assert any provider's bucketing implementation.

```go
package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
)

type Policy struct {
	Key      string
	Revision string
	Enabled  bool
	Percent  int
}

type Decision struct {
	RunID     string `json:"run_id"`
	Tenant    string `json:"tenant"`
	Region    string `json:"region"`
	FlagKey   string `json:"flag_key"`
	Revision  string `json:"revision"`
	Percent   int    `json:"percent"`
	Bucket    uint32 `json:"bucket"`
	Candidate bool   `json:"candidate"`
}

func evaluate(p Policy, tenant, region, runID string) (Decision, error) {
	if p.Key == "" || p.Revision == "" || tenant == "" || runID == "" {
		return Decision{}, fmt.Errorf("missing decision identity")
	}
	if region != "us" && region != "eu" {
		return Decision{}, fmt.Errorf("unknown cohort region: %q", region)
	}
	if p.Percent < 0 || p.Percent > 100 {
		return Decision{}, fmt.Errorf("percentage outside 0..100")
	}
	h := fnv.New32a()
	if err := json.NewEncoder(h).Encode([]string{p.Key, region, tenant}); err != nil {
		return Decision{}, err
	}
	bucket := h.Sum32() % 10000
	return Decision{
		RunID: runID, Tenant: tenant, Region: region,
		FlagKey: p.Key, Revision: p.Revision, Percent: p.Percent,
		Bucket: bucket,
		Candidate: p.Enabled && bucket < uint32(p.Percent*100),
	}, nil
}

func main() {
	p := Policy{Key: "import-candidate-eu", Revision: "r7", Enabled: true, Percent: 5}
	d, err := evaluate(p, "tenant-042", "eu", "import-00017")
	if err != nil {
		panic(err)
	}
	out, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}
```

Save this as a Go source file and run it with `go run`. The output is one decision record. Production integration must persist that record before dispatch and route a false candidate decision to the established importer; this small program performs neither dispatch nor storage.

There is a capacity trap in the percentage label: 5% of tenants is not necessarily 5% of imported repositories, bytes, or worker time. Before increasing a stage, estimate the admitted tenants' queued work and the overlap with retries, then check worker concurrency and downstream limits against that demand. Tenant stability helps incident reconstruction, but it cannot promise an even distribution of load. Large tenants deserve explicit review before admission. For incident reconstruction, preserve the dispatch decision separately from the latest control-plane snapshot, because an operator inspecting a 25% setting after a rollback may otherwise assign an earlier run to the wrong population. A release record should connect the actor, intended setting, change time, observed revision, and affected run IDs without making the current percentage stand in for historical evidence.

Capture before changing.

The separate Go program below retrieves a flag snapshot through the service's REST API. Set `INFRAI_BASE_URL` to its documented versioned HTTPS API base, `INFRAI_API_KEY` to your credential, and `FLAG_KEY` to an existing flag key. Save it in its own directory as a Go source file and run `go run` on that file. It uses a read-only request, checks status, bounds retries, and prints the returned JSON without guessing response fields. This snapshot supplements the per-run evidence; it cannot replace it.

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

func fetchFlag() error {
	base, key, flag := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY"), os.Getenv("FLAG_KEY")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || key == "" || flag == "" {
		return fmt.Errorf("set an HTTPS API base, API key, and flag key")
	}
	endpoint := strings.TrimRight(base, "/") + "/flags/get/" + url.PathEscape(flag)
	client := &http.Client{Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			retry := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			} else if deadline, err := http.ParseTime(retry); err == nil {
				delay = time.Until(deadline)
			}
			if delay < 0 {
				delay = 0
			}
			if delay > time.Minute {
				return fmt.Errorf("rate limited; retry later after %s", delay)
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("flag read returned %s: %s", resp.Status, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("flag read exhausted retries")
}

func main() {
	if err := fetchFlag(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

## Buy versus build around the missing-result signal

Choose the release control and the deadline detector as separate responsibilities, even if the team later buys them together. OpenTelemetry's metrics concepts provide a vocabulary for the freshness SLI; a metric definition still needs an expected schedule and an owner for the alert. Healthchecks belongs in the comparison because missing scheduled activity is the concrete failure under discussion, while LaunchDarkly and Unleash belong because the release needs feature flag control.

| Option | Buy versus build decision | Boundary to verify for this runbook |
| --- | --- | --- |
| LaunchDarkly | Evaluate a dedicated feature management product when release control warrants a separate service. | Confirm the chosen configuration preserves cohort identity and exposes the change evidence your incident process requires; retain application result deadlines. |
| Unleash | Evaluate a feature management option when deciding how much platform operation the team should own. | Assess hosting responsibilities and evaluation behavior for the selected deployment; owning the service also requires staffing its operation. |
| Healthchecks | Evaluate a scheduled-job monitoring tool for the missing-completion signal. | Align the monitored completion with a committed import result, and set the expected schedule and grace period deliberately. It does not choose the candidate cohort. |
| Infrai | Consider its simple percentage flags behind one REST API with a single API key when a stable capability contract is the desired boundary between application code and the vendor behind it. Public, self-describing discovery also helps inspect the integration contract during release preparation. | Not suitable as the sole incident-reconstruction system: retain application-owned decision evidence and a separate scheduled-completion monitor. |
| Sentry | Evaluate an error-focused investigation workflow when captured failures are the useful starting point. | A captured error and a missing expected result are different evidence; check scheduled-job requirements independently. |
| Datadog | Evaluate a broader observability platform when the team wants to investigate the worker and its surrounding infrastructure together. | Assess instrumentation scope and operational ownership; do not substitute infrastructure health for result completion. |
| Grafana | Evaluate a dashboard-centered workflow when the team already owns the underlying telemetry pipeline. | The trade-off is ownership of data sources and the expected-run model; a dashboard alone cannot establish which manifests should exist. |
| Application-owned gate | Build the narrow evaluator when stable cohort admission is the whole requirement. | The team owns configuration distribution, operator authorization, change records, and the deadline detector. The short hash function is a small part of that responsibility. |

The table is a scope comparison, not a benchmark or an assertion that the products share an identical API. A provider change should leave application code alone only when the capability contract and its required semantics stay compatible; verify that assumption with decision fixtures before switching the implementation behind the boundary.

For this workflow, simple percentage release control is sufficient only if the platform team accepts ownership of the surrounding evidence.

That ownership costs attention.

Treat experiment analysis, evaluation statistics, dependency orchestration, and governed change history as explicit selection requirements whenever they matter. Do not infer them from the existence of a percentage setting. Polling clients also need an application policy for configuration freshness, since a changed setting and a worker observing that setting are different events.

## Verify the deadline, then rehearse rollback

Before customer admission, exercise four cases in a nonproduction environment: the schedule never launches, the worker starts without committing a manifest, the source returns a valid empty result, and the candidate flag is off. The first two should violate the expected-result deadline; the empty result should satisfy it when committed correctly; the off case should still complete through the established path. A completion ping sent at worker start defeats this exercise by declaring success before results exist.

Next, hold a tenant and flag key constant while changing only the run ID, and confirm that the example's bucket stays constant. Compare the captured revision with the operator's release record, then increase and decrease the percentage while checking the admission boundary. Separately verify the actual provider or adapter behavior used by the backend; passing the local example's checks says nothing about another evaluator's algorithm.

Rollback needs a deadline too. Define how quickly workers must observe the off setting, measure that propagation in rehearsal, and preserve the prior decision records rather than overwriting them with the latest state. Stop admitting new candidate runs first. Identify work already running before deciding whether to drain or cancel it, and require idempotent result commits before replaying an import through the established implementation.

Keep watching result freshness after the setting changes. A successful configuration write cannot prove that the affected imports recovered, and a quiet candidate cohort cannot prove that the queue drained. Advance the release only when the expected manifests arrive within the chosen SLO and the on-call engineer can reconstruct why each affected tenant used its particular path.

## References

- [OpenTelemetry metrics concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
