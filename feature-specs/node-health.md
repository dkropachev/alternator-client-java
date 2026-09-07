# Node health

This specification defines how an Alternator client tracks endpoint health, selects endpoints for
requests and retries, verifies recovering endpoints, and interacts with topology discovery and
route affinity. It is intentionally language agnostic so the same behavior can be implemented by
other Alternator clients.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Node health protects requests from endpoints that repeatedly fail before returning an HTTP
response, while allowing excluded endpoints to recover without a client restart. It must also avoid
removing healthy capacity because of application errors, authentication errors, or ambiguous
server-side failures.

The feature covers:

- health states and transitions;
- admission quarantine and direct validation for unknown endpoints;
- traffic and probe outcome classification;
- request and retry routing;
- quarantine fallback and recovery;
- topology-discovery interaction; and
- concurrency and lifecycle behavior.

The feature does not persist health history across process restarts. Configured seeds and newly
discovered endpoints therefore enter quarantine until this client directly verifies them.

## Vocabulary and implementation mapping

Portable specifications use the terms in the first column. The final column connects those terms
to this Java implementation without making the Java names normative for other clients.

| Term | Portable meaning | Java implementation |
| --- | --- | --- |
| Endpoint | A network destination for one Alternator node, identified by scheme, host, and effective port. | `URI` values managed by [`AlternatorLiveNodes`](../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) |
| Canonical endpoint | The normalized identity used to recognize equivalent endpoint spellings, including implicit and explicit default ports. | `NodeHealthStore.canonicalNodeKey(...)` in [`NodeHealthStore`](../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) |
| Discovered set | Endpoints returned for the effective routing scope by topology discovery. | `discoveredNodes` in `AlternatorLiveNodes` |
| Health state | The routing classification `ACTIVE`, `QUARANTINED`, or `DOWN`. | [`NodeHealthState`](../src/main/java/com/scylladb/alternator/NodeHealthState.java) |
| Status | A snapshot containing state, relevant consecutive counters, last-update time, and the current attempt generation. | [`NodeHealthStatus`](../src/main/java/com/scylladb/alternator/NodeHealthStatus.java) |
| Health store | Per-client mutable state and state-transition logic keyed by canonical endpoint. | `NodeHealthStore` |
| Health manager | Per-client coordination of health state, direct-probe admission, deduplication, prioritization, timeouts, and lifecycle. | [`NodeHealthManager`](../src/main/java/com/scylladb/alternator/internal/NodeHealthManager.java) |
| Observation | A classified traffic or probe result that is allowed to update health. | [`NodeHealthObservation`](../src/main/java/com/scylladb/alternator/NodeHealthObservation.java) |
| Attempt generation | A per-endpoint token captured when traffic is routed and incremented whenever the endpoint enters `DOWN`. It prevents a late result from an older health cycle from changing current health. | `NodeHealthStatus.getGeneration()` and the generation-aware `AlternatorLiveNodes.reportNodeResult(...)` overload |
| Health-neutral response | A response that updates transmission bookkeeping but produces no health observation and changes no health counter or timestamp. | The retryable-server-status branch in [`BasicQueryPlanInterceptor`](../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) |
| Admission quarantine | Initial validation of a configured seed or newly discovered endpoint. | A node added through `NodeHealthStore.addQuarantinedNode(...)` |
| Recovery quarantine | Verification after a `DOWN` endpoint passes enough recovery probes. | `QUARANTINED` after down-node recovery |
| Direct validation probe | A direct `GET /localnodes` request whose HTTP 200 response activates a quarantined endpoint. | `AlternatorLiveNodes.probeQuarantinedNodes()` or a successful discovery contact |
| Base query plan | Request-scoped candidate order that contains every discovered endpoint once and does not interpret health. See [Query plans](query-plan.md). | [`LazyQueryPlan`](../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) |
| Health-aware query plan | Request-scoped wrapper that applies final health eligibility without rebuilding the base order. | [`NodeHealthQueryPlan`](../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) |
| Logical query | One user-visible DynamoDB operation, regardless of the number of SDK transmission attempts. | One health-aware plan created during `beforeExecution` by `BasicQueryPlanInterceptor` |
| Attempt | One transmission of a logical query to one endpoint. | Per-transmission routing through `routeAttempt(...)` and the sync/async attempt-routing HTTP wrappers |
| Traffic cycle | One traversal that returns each currently eligible canonical endpoint at most once before traffic routing may repeat endpoints. | One lifetime of the `tried` set in `NodeHealthQueryPlan` |
| Regular plan | Non-deterministic traffic plan whose source order is filtered into active and quarantine passes. | Unseeded `LazyQueryPlan` wrapped by `NodeHealthQueryPlan` |
| Affinity plan | Deterministic traffic plan calculated over every discovered endpoint, including down endpoints, then filtered into active and quarantine passes. | Seeded or preferred `LazyQueryPlan` wrapped by `NodeHealthQueryPlan` |
| Probe plan | Control-plane plan that prefers active endpoints and falls back to quarantined endpoints. | `NodeHealthQueryPlan.Mode.PROBE` |

## Health states

### Active

An active endpoint is eligible for ordinary traffic. Consecutive traffic failures move it to
`DOWN` at the configured active failure threshold.

### Quarantined

A quarantined endpoint is excluded during a request's active pass. After that request exhausts its
active candidates, quarantined endpoints are eligible in their source-plan relative order. Direct
control-plane probes validate quarantined endpoints independently of DynamoDB traffic.

Quarantine covers two situations:

- **admission quarantine**, for configured seeds and newly discovered endpoints that this client has
  not contacted successfully; and
- **recovery quarantine**, for endpoints returning from `DOWN`.

Both have the same routing eligibility and transitions. A successful direct validation probe
activates either kind of quarantined endpoint immediately. DynamoDB traffic uses the configured
quarantine promotion and failure thresholds.

### Down

A down endpoint must not receive new DynamoDB traffic. It may receive explicit recovery probes.
Traffic outcomes from attempts scheduled before it became down are stale and must not alter its
state or counters.

## Inputs and outcome classification

### DynamoDB traffic

Each completed attempt must be classified as follows:

| Attempt outcome | Health action |
| --- | --- |
| Final HTTP response other than `500`, `502`, `503`, or `504` | Report `TRAFFIC_SUCCESS`. |
| HTTP `500`, `502`, `503`, or `504` | Report nothing. Clear in-flight bookkeeping so the response cannot later be mistaken for a transport failure. |
| Failure before any HTTP response | Report `TRAFFIC_FAILURE`. |

Application, validation, authentication, conditional, and throttling responses are successful
health contacts even when the logical query fails. They prove that the endpoint processed the
request far enough to return a classified response.

The retryable server statuses are health-neutral because Alternator may report coordinator
timeouts, cluster-wide overload effects, and unrelated internal failures using the same
`InternalServerError` type. Treating every such response as a node failure could remove capacity
during overload. Treating it as a success would incorrectly reset existing failure progress.

A health-neutral response must not update health counters, state, or health-update time.

### Control-plane probes

A health probe sends `GET /localnodes` directly to the target endpoint:

| Probe outcome | Health action |
| --- | --- |
| HTTP 200 | Report `PROBE_SUCCESS`. |
| Any other HTTP status | Report `PROBE_FAILURE`. |
| Failure before an HTTP response | Report `PROBE_FAILURE`. |

Probe outcomes must not demote an active endpoint. A successful direct probe activates a
quarantined endpoint and initializes its active counters; a failed direct probe leaves its state and
traffic counters unchanged. A discovery or probe failure does not count as a DynamoDB traffic
failure.

Standalone health probes classify `/localnodes` responses by HTTP status and do not parse the
response body. Topology discovery also requires a syntactically valid response body before it
reports a successful contact; a valid empty array is still a successful direct contact.

Each background health-probe cycle takes snapshots of eligible down and quarantined endpoints and
submits both kinds of work without waiting for either kind to finish. Admitted down-node work has
higher queue priority than background quarantine validation. A down endpoint that reaches recovery
quarantine becomes eligible for direct validation in a later cycle; it is not added to the
quarantine snapshot already taken for the current cycle. Explicit `probeQuarantinedNodes()` calls
run quarantine validation immediately without waiting for the next cycle.

Health probes run through a bounded priority executor. Explicit probes have highest priority,
followed by down-node recovery and background quarantine validation. One canonical endpoint may
have at most one physical probe in flight; concurrent requests share its result. Background cycles
submit work without waiting. Explicit synchronous calls return, and explicit asynchronous futures
complete, only after their own snapshot results settle. Per-node failures do not fail an explicit
batch.

The per-probe timeout starts when a worker begins the request. Timeout reports probe failure and
aborts the prepared request. A late response after timeout is ignored. Transport implementations
must honor request abortion for timeout and shutdown to release blocked workers promptly.

## State transitions

### Active endpoints

| Input | Transition |
| --- | --- |
| `TRAFFIC_SUCCESS` | Remain `ACTIVE`; reset the active traffic-failure streak. |
| `TRAFFIC_FAILURE` below threshold | Remain `ACTIVE`; increment the active traffic-failure streak and reset recovery-success state. |
| `TRAFFIC_FAILURE` reaching threshold | Move to `DOWN`; reset quarantine-failure and recovery-success state. |
| Health-neutral response | No state or counter change. |
| Either probe outcome | No state or traffic-counter change. |

### Quarantined endpoints

| Input | Transition |
| --- | --- |
| `TRAFFIC_SUCCESS` below promotion threshold | Remain `QUARANTINED`; increment the promotion streak and reset the quarantine-failure streak. |
| `TRAFFIC_SUCCESS` reaching promotion threshold | Move to `ACTIVE`; initialize active counters as healthy. |
| `TRAFFIC_FAILURE` below quarantine failure threshold | Remain `QUARANTINED`; reset promotion progress and increment the quarantine-failure streak. |
| `TRAFFIC_FAILURE` reaching quarantine failure threshold | Move to `DOWN`. |
| Health-neutral response | No state or counter change. |
| `PROBE_SUCCESS` | Move immediately to `ACTIVE`; initialize active counters as healthy. |
| `PROBE_FAILURE` | Remain `QUARANTINED` with no counter change. |

### Down endpoints

| Input | Transition |
| --- | --- |
| `PROBE_SUCCESS` below recovery threshold | Remain `DOWN`; increment the probe-recovery streak. |
| `PROBE_SUCCESS` reaching recovery threshold | Move to recovery quarantine with zero promotion and quarantine-failure progress. |
| `PROBE_FAILURE` | Remain `DOWN`; reset the probe-recovery streak. |
| Any traffic result | Ignore it as stale; do not change state or counters. |

Entering recovery quarantine never promotes a node directly to active. A subsequent successful
DynamoDB promotion sequence or direct validation probe is required.

### Attempt generations and stale traffic

Every endpoint starts with attempt generation zero. Entering `DOWN` increments its generation,
including each later transition to `DOWN` after a recovery cycle. A transport must capture the
current generation at the final eligibility check for each DynamoDB attempt and submit that captured
generation with the attempt's health observation.

A traffic observation is accepted only when its captured generation equals the endpoint's current
generation. This rule rejects results from attempts sent before the endpoint entered `DOWN`, even if
the result arrives after that endpoint has recovered to quarantine or active. Rejected stale results
must not change state, counters, or the health-update time. Probe observations do not use attempt
generations.

## Endpoint admission and direct validation

Endpoint admission uses direct client-to-endpoint evidence instead of trusting an endpoint merely
because another node returned it from topology discovery:

1. Configured seeds start quarantined and remain eligible as bootstrap candidates.
2. A quarantined seed or other quarantined discovery candidate that successfully returns HTTP 200
   from `/localnodes` becomes active. A standalone direct validation probe checks status only. A
   topology-discovery contact additionally requires a parseable response body; an empty array is
   valid and activates the contacted endpoint.
3. Endpoints contained in a discovery response are added in quarantine unless the client already
   has a health record for them. Being reported by another node does not activate them.
4. Background health cycles and `probeQuarantinedNodes()` directly send `GET /localnodes` to each
   quarantined endpoint in the current discovered set. Endpoints returning HTTP 200 become active;
   failures remain quarantined.
5. Before the first successful topology update, the discovered set consists of configured bootstrap
   seeds, so an explicit probe operation targets those seeds.
6. Discovery never overwrites an established health state. An endpoint removed and later
   rediscovered during the same client lifetime retains its previous state and counters.

If all candidates are quarantined, they are routed in base-plan order under the no-active-endpoint
rule. Startup therefore does not wait for topology refresh or explicit validation.

## Configuration and defaults

| Setting | Default | Meaning |
| --- | ---: | --- |
| Active failure threshold | 10 | Consecutive traffic failures required to move `ACTIVE` to `DOWN`. |
| Down recovery success threshold | 3 | Consecutive successful down-node probes required to enter recovery quarantine. |
| Quarantine promotion threshold | 10 | Consecutive successful traffic contacts required to move `QUARANTINED` to `ACTIVE` without a direct probe. |
| Quarantine failure threshold | 3 | Consecutive traffic failures required to move `QUARANTINED` to `DOWN`. |
| Background health-probe period | 30 seconds | Period between cycles that probe down and quarantined endpoints. Must be positive. |
| Health-probe concurrency | 4 | Maximum concurrent direct health probes. Must be between 1 and 64. |
| Health-probe timeout | 5 seconds | Total deadline after an individual probe starts running. Must be positive. |

Thresholds below one are normalized to one. A non-positive background health-probe period is invalid
because it could make `DOWN` terminal and quarantine permanent without user traffic. Disable the
complete node-health feature instead of disabling background probes.

The Java configuration retains deprecated quarantine traffic-interval and traffic-idle accessors
for source compatibility. Their values no longer affect routing.

Built-in polling transports reserve one connection beyond health-probe concurrency for topology and
feature-check traffic. An externally supplied polling transport must support concurrent calls and
request abortion.

In Java, `AlternatorConfig.Builder.withNodeHealthConfig(...)` installs this configuration and
`withNodeHealthDisabled()` disables the feature. The synchronous and asynchronous client builders
provide the same two methods. Client wrappers expose synchronous and asynchronous quarantine-probe
operations, while `AlternatorLiveNodes` additionally exposes state snapshots and health reporting
for custom integrations. Java's built-in traffic integration uses the generation-aware
`reportNodeResult(...)` overload. A custom integration that can have concurrent or late traffic
results must capture `getNodeHealthGeneration(...)` at final routing and use that overload as well.
`AlternatorLiveNodes` owns topology discovery and delegates health state and probe orchestration to
`NodeHealthManager`.

For source and behavioral compatibility, Java's existing `getLiveNodes()` method remains a raw
discovered-topology view and is equivalent to `getDiscoveredNodes()`; it must not silently become a
health-filtered view. Callers use `getActiveNodes()`, `getQuarantinedNodes()`, and `getDownNodes()`
when they need explicit health partitions.

## Health-aware routing

The base query plan must remain health agnostic and produce every discovered candidate at most once
in its chosen random, affinity, or preferred order. A request-scoped health-aware wrapper applies
eligibility at the last routing moment.

The wrapper must:

- recheck current health on every selection, including retries;
- capture the base-plan order lazily on first selection;
- scan for untried active candidates in base-plan relative order before considering quarantine;
- when no untried active candidate exists, scan the same order for untried quarantined candidates;
- reject down candidates in both passes; and
- return each canonical endpoint at most once per traffic cycle.

After both passes are exhausted, regular and affinity traffic plans clear their tried-endpoint set
and begin another cycle only when the SDK requests another attempt. Regular plans reshuffle before
the new cycle; affinity plans reuse their exact deterministic order. No separate request-endpoint
fallback is used. If only down endpoints remain, routing fails locally without sending traffic.

### Regular plans

A regular plan creates a non-deterministic permutation of the discovered endpoint snapshot. Its
active pass therefore has random active-node order, and its quarantine fallback pass has random
quarantined-node order. After each complete cycle it reshuffles the same snapshot. Quarantine is
never injected before active candidates.

### Affinity plans

An affinity plan calculates its deterministic order over the complete discovered set, including
active, quarantined, and down endpoints. Selection returns its untried active subsequence first,
then its untried quarantined subsequence from the exact same order. Health changes therefore do not
alter affinity hashing or the underlying permutation.
After a complete cycle, another SDK attempt restarts at the beginning of that permutation while
continuing to apply current endpoint state.

### Probe plans

A probe plan must try active candidates first and quarantined candidates second, exclude down
candidates, and stop after one traversal without starting another cycle.

## Interactions with other features

### Topology discovery and routing scopes

Health state is separate from topology membership. Discovery determines which endpoints belong to
the current routing ring; health determines which of those endpoints may receive traffic.

Discovery fallback across rack, datacenter, and cluster scopes continues until a non-empty result is
found or every configured scope is exhausted. A topology response may omit an endpoint without
deleting its health history. Initial seeds remain eligible for topology-discovery fallback and
explicit down-node recovery probes even when they are absent from the current discovered ring.

For both the current discovered pool and the later configured-seed fallback pool, topology refresh
partitions candidates by current state, independently randomizes each partition, and tries them in
`ACTIVE`, `QUARANTINED`, then `DOWN` order. Down endpoints are control-plane fallback candidates:
discovery results from them do not advance or reset down-node recovery counters.

### SDK retries

One logical query owns one base plan and one health-aware wrapper across all attempts. Each retry
selects the next eligible candidate and reports the preceding attempt exactly once.

Receiving a health-neutral server response must clear the in-flight endpoint without reporting a
health result. Otherwise, the next attempt could incorrectly report the previous response as a
transport failure.

### Key-route affinity and batch affinity

Endpoint eligibility must not alter affinity hashing, voting, or the underlying candidate order.
Affinity is calculated over the stable discovered set, including quarantined and down endpoints.
The active and quarantine passes preserve their respective subsequences of that order.

The complete affinity contract is defined in [Key-route affinity](key-route-affinity.md).

Requests that do not qualify for affinity, including unsupported keys or operations, use regular
routing. Batch-affinity voting identifies preferred candidates before final endpoint eligibility is
applied.

### Error handling and response compression

Health classification uses the final HTTP status or absence of a response, not a parsed application
exception. Reading or classifying a response must not consume, replace, or corrupt the response body
used by SDK unmarshalling or compression handling.

### Synchronous and asynchronous clients

Synchronous and asynchronous transports must produce identical routing and health observations.
The transport integration may differ, but it must preserve one plan per logical query and one result
per completed attempt.

### Feature checks and other control-plane traffic

`/localnodes` feature detection uses a probe plan; topology refresh directly iterates its randomized
health-tiered discovery candidates. These control-plane requests are independent of DynamoDB query
plans and must not alter traffic counters. A successful direct request may activate a quarantined
endpoint.

### Probe concurrency and lifecycle

Probe work is deduplicated by canonical endpoint across background and explicit callers. A bounded
queue admits at most seventeen times the configured concurrency including running jobs. Excess
background work is retried by a later cycle; an explicit call fails when capacity is exhausted.
Queued explicit work may upgrade an existing queued background job. Cancelling an aggregate explicit
future does not cancel endpoint work shared with other callers. An explicit call that begins after a
shared result is already complete must not reuse that completed result: it waits for physical cleanup
and submits a fresh probe if the endpoint remains eligible.

Background admission must avoid starving either health tier or nodes near the end of a large
candidate set. When both down and quarantined work exist, a cycle shares its currently available
capacity between the two tiers, alternates the tier that receives a sole available slot, rotates the
starting endpoint within each tier across cycles, and reassigns capacity that one tier cannot use.
Once admitted, executor priority remains explicit probes, down-node recovery, then background
quarantine validation.

Successful DynamoDB traffic on a quarantined endpoint suppresses one queued or upcoming background
quarantine probe. A worker already running that endpoint probe continues and its result applies.
Explicit probes ignore suppression. Traffic failure or transition out of quarantine clears pending
suppression.

Shutdown rejects new probes, cancels queued work without reporting health failure, and aborts running
control-plane requests. The bounded `shutdownAndWait` operation waits for the live-node thread and
both probe executors within one shared timeout. A polling transport that ignores abortion may cause
bounded shutdown to report failure.

### Disabled node health

When node health is disabled, all discovered endpoints are treated as active, no quarantine or down
sets are exposed, outcome reports do not change state, and health-aware filtering must not exclude
an endpoint. Other routing features continue to operate normally.

## Interaction edge cases

- A discovery response never activates the endpoints listed in its body; it activates only the
  endpoint contacted directly when that request succeeds.
- An empty configured scope followed by a successful fallback scope publishes only the successful
  non-empty fallback result. Every newly listed endpoint starts quarantined.
- A retryable server response from a quarantined endpoint leaves it in the same state with unchanged
  progress.
- If an active candidate becomes down before final selection, it is skipped.
- If active candidates disappear during retries, the quarantine pass remains available after the
  active pass is exhausted.
- Each selection evaluates current state. An endpoint already returned in the current traffic cycle
  is not returned again until every currently eligible candidate has been tried.
- A successful control-plane request sent directly to a quarantined endpoint activates it but does
  not advance traffic-promotion counters.
- A topology refresh may omit an active endpoint and later restore it; restoration must reveal the
  retained health state rather than creating a new active status.
- If every active and quarantined discovery candidate fails, topology refresh may read topology from
  a down endpoint without making it eligible for DynamoDB traffic or changing recovery progress.
- A node returned with an explicit default port and the same node returned with an implicit default
  port share one health record and one probe per cycle.
- A quarantined endpoint removed while its probe runs cannot be activated by that result; topology
  publication and quarantine-result application use one lock.
- A successful DynamoDB contact skips a queued background quarantine probe once but does not cancel
  a probe that already started.
- A down node that reaches recovery quarantine during a background cycle is considered for direct
  quarantine validation on a later cycle, because both candidate snapshots are taken before work is
  submitted.
- Timeout, shutdown, and HTTP completion race through one terminal decision; at most one observation
  updates health.
- A response received after another concurrent attempt marks the endpoint down carries an older
  attempt generation and must not resurrect it, even if recovery completed before that response
  arrived.

## Regular logic edge cases

- Active candidates are always returned before quarantined candidates for one logical query.
- With no active candidates, all quarantined endpoints are routable in source-plan relative order.
- With only down endpoints, selection returns no route.
- A base plan containing null, duplicate, removed, or dynamically ineligible candidates must not
  return an invalid route or throw because of health filtering.
- A conforming traffic plan returns each canonical discovered endpoint at most once per cycle.
- When no untried active or quarantined candidate remains, traffic routing clears the tried set and
  scans once for a new cycle; if that scan also finds nothing, all snapshot candidates are down and
  selection returns no route.
- A quarantined traffic failure resets promotion progress even when the endpoint remains
  quarantined below its failure threshold.
- Probe failures reset only down-node probe-recovery progress.
- Health-neutral server responses neither break nor advance an existing consecutive-failure streak.
- Background health probing cannot be disabled independently; its period must remain positive
  whenever node health is enabled.

## Conformance tests

The Java implementation organizes conformance coverage as follows:

| Area | Test location |
| --- | --- |
| Configuration defaults, normalization, and invalid probe periods | [`NodeHealthConfigTest`](../src/test/java/com/scylladb/alternator/NodeHealthConfigTest.java) |
| State transitions, direct-probe activation, counter independence, canonical endpoints, and disabled behavior | [`NodeHealthStoreTest`](../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java) |
| Active and quarantine passes, regular and affinity ordering, probe plans, and dynamic retry transitions | [`NodeHealthQueryPlanTest`](../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) |
| Admission quarantine, explicit validation probes, discovery, down-node probes, rediscovery, scope behavior, and control-plane isolation | [`AlternatorLiveNodesNodeHealthTest`](../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java) |
| Probe concurrency, timeout, suppression, topology races, and shutdown rejection | [`AlternatorLiveNodesConcurrentProbeTest`](../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesConcurrentProbeTest.java) |
| Per-attempt retry routing, transport reports, authentication responses, and health-neutral server statuses | [`RetryDistributionTest`](../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) |
| Final-gate revalidation before the first physical transmission | [`BasicQueryPlanInterceptorTest`](../src/test/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptorTest.java) |
| Polling-loop resilience, probe scheduling during refresh failures, and bounded shutdown | [`AlternatorLiveNodesShutdownTest`](../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesShutdownTest.java) |
| Affinity request classification, batch voting, random fallback, and down preferred candidates | [`AffinityQueryPlanInterceptorTest`](../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) |
| Stable cross-language base-plan ordering | [`LazyQueryPlanCrossLanguageTest`](../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java) |
| Default configuration compatibility | [`AlternatorConfigCompatibilityTest`](../src/test/java/com/scylladb/alternator/AlternatorConfigCompatibilityTest.java) |
| Sync and async builder propagation | [`AlternatorDynamoDbClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientCustomizerTest.java) and [`AlternatorDynamoDbAsyncClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbAsyncClientCustomizerTest.java) |
