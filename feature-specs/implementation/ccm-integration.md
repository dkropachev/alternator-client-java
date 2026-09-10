# CCM integration implementation

This document connects the [CCM-integration specification](../ccm-integration.md) to this
repository's Java test harness.

## Public API

[`TestClusters`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusters.java) exposes
`acquireReusable(ClusterSpec)` and `provisionPrivate(ClusterSpec)`. Both return `AutoCloseable`
leases. `ReusableClusterLease` exposes read-only cluster information and a resource scope;
`PrivateClusterLease` additionally exposes `PrivateClusterControl`.

[`ClusterSpec`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpec.java) provides
immutable `with...` methods for version, topology, transports, security, per-node resources, and
YAML overrides. Override keys are canonicalized and values are parsed as YAML 1.2.
`ClusterSpecs.defaultSpec()` applies `SCYLLA_VERSION`, defaulting to `release:2025.2.5`.

The operational environment is intentionally small: `SCYLLA_CCM_PATH` selects CCM,
`SCYLLA_CCM_ROOT` optionally selects the private state root, `SCYLLA_CCM_MAX_NODES` may lower the
nine-node ceiling, and `SCYLLA_CCM_DIAGNOSTICS_DIR` selects the external artifact directory. The
default root is shared per user; concurrently running processes must select the same override root
to coordinate address reservations.

## Internal architecture

[`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java) owns
one physical-cluster slot, lease reference counts, resource scopes, CCM address reservations,
normal shutdown, and next-start stale-run recovery. Matching reusable acquisitions share the slot.
An idle incompatible cluster is removed and replaced; incompatible acquisitions fail immediately
while leases are active. Operations are serialized directly rather than through an admission
scheduler, wait queue, memory budget, or LRU cache.

[`CcmProvisioner`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisioner.java)
translates topology into CCM create/add operations, supplies typed Scylla YAML, generates a CA and
node certificates with OpenSSL, waits for HTTP and HTTPS readiness, and writes each command to its
own durable log. A failed or ambiguous mutation dirties the cluster, which prevents further reuse
or mutation until whole-cluster cleanup.

## Lifecycle and concurrency

The pool creates each run and address reservation under the single configured root and records its
owner identity and cluster manifest before invoking CCM. Processes using distinct override roots do
not coordinate address selection. Startup recovery distinguishes a live owner using PID,
process start ticks, and boot ID; terminates same-user processes carrying that run's marker; copies
diagnostics; and performs bounded CCM removal. A run and its address reservation are deleted only
after cleanup succeeds. Malformed or unremovable runs remain quarantined while a new run selects a
different ID.

[`IntegrationTestConfig`](../../src/test/java/com/scylladb/alternator/IntegrationTestConfig.java)
holds one reusable default lease for the existing integration suite. The Makefile only validates
the pinned CCM entry point and invokes two foreground Maven phases with `forkCount=1`; Java owns all
run lifecycle and recovery behavior. The Surefire listener calls `TestClusters.closeAll()` during
normal suite shutdown.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `CCM-REQ-001` | [`ClusterSpec`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpec.java) | [`ClusterSpecValidationTest#validatesEverySecurityModeCombination`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpecValidationTest.java) | `conformant` |
| `CCM-REQ-002` | [`CcmProvisioner`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisioner.java) | [`ClusterProvisioningIT#authorizedClusterProvidesWorkingCredentials`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-003` | [`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java) | [`ClusterProvisioningIT#sameSpecReusesClusterWithIndependentResourceScopes`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-004` | [`PrivateClusterLease`](../../src/test/java/com/scylladb/alternator/testinfra/PrivateClusterLease.java) | [`ClusterProvisioningIT#privateHttpsClusterCanChangeNodeLifecycleAndTopology`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-005` | [`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java) | [`ClusterInfrastructureTest#incompatibleActiveRequestsFailFastAndIdleClustersAreReplaced`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterInfrastructureTest.java) | `conformant` |
| `CCM-REQ-006` | [`CcmRunState`](../../src/test/java/com/scylladb/alternator/testinfra/CcmRunState.java) | [`CcmRunStateTest#hardKilledJvmIsRecoveredByTheNextHarnessStartup`](../../src/test/java/com/scylladb/alternator/testinfra/CcmRunStateTest.java) | `conformant` |

## Test coverage

- `ClusterSpecValidationTest` covers security combinations, topology limits, and canonical YAML
  override keys.
- `ClusterLifecycleTest` covers matching reuse, fail-fast incompatible requests, private
  exclusivity, dirty-state handling, idempotent close, and bounded resource cleanup.
- `ClusterInfrastructureTest` covers state-root validation, address reservations, and bind-based
  address checks. `CcmRunStateTest` covers owner identity, reservation quarantine, and a subprocess
  hard-kill followed by real provisioner cleanup, diagnostic preservation, and reprovisioning.
- `CcmProvisionerRecoveryTest` covers durable command logs, cancellation, rollback, and diagnostic
  snapshots.
- `CcmMakefileTest` covers the two foreground test phases, environment propagation, cached CCM
  validation, concurrent installation locking, and phase-failure propagation.
- `ClusterProvisioningIT` proves real-cluster reuse, independent resource scopes, enforced
  authorization, custom-CA HTTPS, YAML overrides, and private node lifecycle operations.

Recovery is deliberately best effort. An uncatchable JVM kill leaves the run in place until the
next harness startup. If CCM cannot load malformed metadata, the run and address ID stay
quarantined rather than being guessed at or removed unsafely.

## Known conformance gaps

None currently known. Malformed CCM state that CCM itself cannot remove remains deliberately
quarantined; this is the specified safe outcome rather than destructive best-effort deletion.
