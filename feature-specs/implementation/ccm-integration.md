# CCM integration implementation

This document connects the [CCM-integration specification](../ccm-integration.md) to the
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`TestClusters`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusters.java) exposes
`acquireReusable(ClusterSpec)` and `provisionPrivate(ClusterSpec)`. Both return `AutoCloseable`
leases for try-with-resources. `ReusableClusterLease` exposes read-only cluster information and a
resource scope; `PrivateClusterLease` additionally exposes `PrivateClusterControl`.

[`ClusterSpec`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpec.java) provides
immutable `with...` methods for version, topology, transports, security, per-node resources, and
YAML overrides. Override keys are canonicalized, topology/address/startup roots owned by typed
options are rejected, and values are parsed as YAML 1.2 before admission.
`ClusterSpecs.defaultSpec()` applies the run's `SCYLLA_VERSION`.

## Internal architecture

[`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java)
owns admission, matching, LRU eviction, lease reference counts, CCM address locks, failed-removal
ownership, and process-wide shutdown. It delegates physical operations to
[`CcmProvisioner`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisioner.java).

The provisioner translates topology into CCM create/add operations, supplies typed Scylla YAML,
generates a CA and node certificates with OpenSSL, waits for HTTP and HTTPS readiness, records every
command, and copies cluster diagnostics before removal.

[`IntegrationTestConfig`](../../src/test/java/com/scylladb/alternator/IntegrationTestConfig.java)
holds one reusable default lease for the existing integration suite. Existing tests therefore read
their endpoints, topology labels, and CA from CCM without per-test Docker configuration.

## Lifecycle and concurrency

The pool reserves nodes and memory before invoking CCM. A matching concurrent acquisition joins the
same completion and increments the lease count. An authoritative ownership registry retains pooled
clusters after they leave the reuse index and until physical removal, capacity release, and address
lock release all succeed. Resource cleanup has a bounded deadline; failed cleanup poisons the
cluster.

Physical mutation methods and removal share a lifecycle state machine. Private controls reserve
capacity around node addition and release it only after successful rollback or removal. Ambiguous
start, stop, and decommission results are reconciled against process and server state. File locks
under the system temporary directory serialize loopback-range allocation across processes.

CCM commands run in isolated process groups. Failure and cancellation terminate descendants before
rollback, and retained PID state lets node and cluster cleanup reap processes omitted from CCM
metadata. The Surefire listener calls `TestClusters.closeAll()` after the run. The Makefile stores
operational state in a private per-user host namespace, validates owner process identity, retries
cleanup after abnormal termination, and transfers diagnostics to `target/ccm` for CI artifacts.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `CCM-REQ-001` | [`ClusterSpec`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpec.java) | [`ClusterSpecValidationTest#validatesEverySecurityModeCombination`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpecValidationTest.java) | `conformant` |
| `CCM-REQ-002` | [`CcmProvisioner`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisioner.java) | [`ClusterProvisioningIT#authorizedClusterProvidesWorkingCredentials`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-003` | [`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java) | [`ClusterProvisioningIT#sameSpecReusesClusterWithIndependentResourceScopes`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-004` | [`PrivateClusterLease`](../../src/test/java/com/scylladb/alternator/testinfra/PrivateClusterLease.java) | [`ClusterProvisioningIT#privateHttpsClusterCanChangeNodeLifecycleAndTopology`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java) | `conformant` |
| `CCM-REQ-005` | [`TestClusterPool`](../../src/test/java/com/scylladb/alternator/testinfra/TestClusterPool.java) | [`ClusterInfrastructureTest#capacityReservesMemoryAndEnforcesLimits`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterInfrastructureTest.java) | `conformant` |
| `CCM-REQ-006` | [`CcmProvisioner`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisioner.java) | [`CcmProvisionerRecoveryTest#timedOutCcmCommandReapsItsProcessGroupBeforeRollback`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisionerRecoveryTest.java) | `conformant` |

## Test coverage

- [`ClusterInfrastructureTest`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterInfrastructureTest.java)
  covers memory calculations, resource naming, failure retention, and cleanup path rejection.
- [`ClusterSpecValidationTest`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterSpecValidationTest.java)
  covers the complete security compatibility matrix and canonical YAML override keys.
- [`ClusterLifecycleTest`](../../src/test/java/com/scylladb/alternator/testinfra/ClusterLifecycleTest.java)
  covers private-control serialization, node reconciliation, capacity accounting, pooled retirement,
  and bounded resource cleanup.
- [`CcmProvisionerRecoveryTest`](../../src/test/java/com/scylladb/alternator/testinfra/CcmProvisionerRecoveryTest.java)
  covers process-tree termination and cleanup of nodes removed from CCM metadata.
- [`CcmMakefileTest`](../../src/test/java/com/scylladb/alternator/testinfra/CcmMakefileTest.java)
  covers Makefile environment propagation, failed-run cleanup, stale-run recovery, diagnostics,
  and cleanup path rejection.
- [`ClusterProvisioningIT`](../../src/integration-test/java/com/scylladb/alternator/ClusterProvisioningIT.java)
  proves real-cluster reuse, independent scopes, enforced authorization, preserved YAML overrides,
  custom-CA HTTPS, and private node add/start/stop/remove/replacement.
- The existing integration-test classes all consume the CCM-backed `IntegrationTestConfig`, covering
  discovery, CRUD, compression, TLS, connection reuse, client implementations, and routing against
  the default three-node cluster.
- `DemoApplicationsIT` launches both demo applications in child JVMs against that cluster, preserving
  their runtime smoke coverage even though the applications terminate their own processes.

## Known conformance gaps

No known conformance gaps are currently recorded.
