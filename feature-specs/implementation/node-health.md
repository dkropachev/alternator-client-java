# Node-health implementation

This document connects the [node-health specification](../node-health.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

[`NodeHealthConfig`](../../src/main/java/com/scylladb/alternator/NodeHealthConfig.java) exposes
thresholds, probe period, concurrency, timeout, and disabled configuration.
[`NodeHealthState`](../../src/main/java/com/scylladb/alternator/NodeHealthState.java),
[`NodeHealthObservation`](../../src/main/java/com/scylladb/alternator/NodeHealthObservation.java),
and [`NodeHealthStatus`](../../src/main/java/com/scylladb/alternator/NodeHealthStatus.java) expose
state, observations, counters, timestamps, and generation snapshots.

Client wrappers expose discovered endpoint views and blocking and asynchronous quarantine-probe
operations. `AlternatorLiveNodes`, available from each wrapper, exposes the health-partitioned views.
`getLiveNodes()` intentionally remains an alias for the raw discovered topology view.

## Internal architecture

[`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) owns
canonical endpoint keys, counters, generations, and transitions.
[`NodeHealthManager`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthManager.java)
coordinates probe admission, priority, deduplication, timeout, suppression, and shutdown.

[`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java)
owns topology discovery and delegates health work to the manager.
[`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java)
applies final active and quarantine passes. Traffic integration belongs to
[`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java).

## Lifecycle and concurrency

Initial seeds are published in quarantine. The live-node polling thread independently schedules
topology refresh and background health cycles. Probe work uses a priority executor and a separate
timeout scheduler. Background capacity, including running jobs, is seventeen times configured
concurrency. Explicit calls may queue their complete snapshot outside that background admission
limit; the fixed worker count still bounds physical concurrency. Background admission splits
available capacity across down and quarantine tiers, rotates starting endpoints, and alternates a
sole slot.

Built-in polling transports reserve one connection beyond probe concurrency. Externally supplied
polling transports must support concurrent calls and abortion. `shutdown()` dispatches cleanup
without blocking its caller. `shutdownAndWait` shares one timeout across cleanup, the polling thread,
and both probe executors.

Traffic routing captures `NodeHealthStatus.getGeneration()` at final eligibility and reports through
the generation-aware overload. While health is enabled, the generation-unaware overload accepts
probe outcomes only and traffic observations require the captured token. Disabled health ignores
all reports.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEALTH-REQ-001` | [`NodeHealthConfig`](../../src/main/java/com/scylladb/alternator/NodeHealthConfig.java) | [`FeatureSpecDefaultsTest#nodeHealthDefaultsMatchSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `HEALTH-REQ-002` | [`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) | [`RetryDistributionTest#testRetryableServerErrorsDoNotReportHealthResults`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |
| `HEALTH-REQ-003` | [`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) | [`NodeHealthStoreTest#downTrafficNeverChangesStateCountersOrUpdateTime`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java) | `conformant` |
| `HEALTH-REQ-004` | [`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) | [`NodeHealthStoreTest#trafficFromGenerationBeforeDownIsIgnoredAfterRecovery`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java) | `conformant` |
| `HEALTH-REQ-005` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) | [`AlternatorLiveNodesNodeHealthTest#discoveryActivatesContactedSeedButQuarantinesNewNodesUntilDirectProbe`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java) | `conformant` |
| `HEALTH-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`NodeHealthQueryPlanTest#regularPlanReturnsActiveThenQuarantineInSourceRelativeOrder`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) | `conformant` |
| `HEALTH-REQ-007` | [`NodeHealthManager`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthManager.java) | [`AlternatorLiveNodesConcurrentProbeTest#explicitBatchLargerThanBackgroundCapacitySettlesWithoutRejection`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesConcurrentProbeTest.java) | `conformant` |
| `HEALTH-REQ-008` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) | [`AlternatorLiveNodesNodeHealthTest#lateDiscoverySuccessCannotActivateRemovedQuarantinedNode`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java) | `conformant` |
| `HEALTH-REQ-009` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) | [`AlternatorLiveNodesShutdownTest#shutdownAndWaitIncludesSlowCleanupInBoundedDeadline`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesShutdownTest.java) | `conformant` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes every machine-readable health default.
- [`FeatureSpecNodeHealthTransitionsTest`](../../src/test/java/com/scylladb/alternator/internal/FeatureSpecNodeHealthTransitionsTest.java)
  executes the portable transition table.
- [`NodeHealthConfigTest`](../../src/test/java/com/scylladb/alternator/NodeHealthConfigTest.java)
  covers defaults, normalization, and invalid probe settings.
- [`NodeHealthStoreTest`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java)
  covers transitions, counter and timestamp independence, canonical identity, generations, stale
  result rejection, and disabled behavior.
- [`AlternatorLiveNodesNodeHealthTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java)
  covers admission, explicit and custom probes, discovery snapshots, recovery, rediscovery, removal
  races, valid empty contacts, disabled behavior, and scope behavior.
- [`AlternatorLiveNodesConcurrentProbeTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesConcurrentProbeTest.java)
  covers concurrency, bounded background admission, complete explicit snapshots, timeout,
  suppression, topology races, and shutdown rejection.
- [`RetryDistributionTest`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java)
  covers per-attempt observations, pre-transport failures, and health-neutral server statuses.
- [`BasicQueryPlanInterceptorTest`](../../src/test/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptorTest.java)
  covers final-gate revalidation and pending-versus-in-flight attempt bookkeeping.
- [`AlternatorLiveNodesShutdownTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesShutdownTest.java)
  covers bounded cleanup, polling interruption, and owned-client closure.

## Known conformance gaps

- Control-plane requests currently serialize implicit ports and IPv6 literals incorrectly in the
  `Host` header. This predates the node-health conformance changes, but direct discovery and health
  probes should use a bracketed authority and omit implicit or standard ports.
