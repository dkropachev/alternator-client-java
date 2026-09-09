# CCM integration

This specification defines a common contract for provisioning native Scylla clusters with CCM for
Alternator integration tests. Cluster identity, resource ownership, admission, and cleanup are
normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

The integration harness supplies real Scylla clusters without requiring a container runtime. It
supports stable default clusters for ordinary tests and isolated clusters for tests that change
topology or process state.

The contract covers cluster description, provisioning, connections, resource namespaces, reuse,
capacity, diagnostics, and cleanup. It does not prescribe the test framework or client language.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Cluster specification | Immutable requested version, topology, transports, security, resources, and configuration overrides. |
| Physical cluster | One named CCM cluster and its node processes. |
| Reusable lease | Shared read-only access to a matching physical cluster. |
| Private lease | Exclusive access to a physical cluster with lifecycle and topology controls. |
| Resource scope | Unique namespace owned by one lease for server-side test resources. |
| Reuse key | Deterministic identity of every setting that affects a physical cluster. |
| Admission | Reservation of physical-node and memory capacity before provisioning. |
| Poisoned cluster | Reusable cluster that failed cleanup or health validation. |

## Configuration and defaults

The default cluster must use a three-node, one-datacenter, one-rack topology; expose HTTP and
HTTPS; use two processing units and 1,024 MiB per node; disable authentication and authorization;
and use `release:2025.2` unless the run selects another version.

A cluster must contain at least one node and no more than nine nodes. Node processing units and
memory must be positive. Alternator authorization enforcement must require non-permissive
authentication and authorization backends.

The scheduler must reserve one quarter of detected available memory, clamped to at least 512 MiB
and at most 4,096 MiB. An explicit node limit may lower but must not raise the nine-node ceiling.
Invalid explicit memory or node limits must fail rather than fall back to detected values.

## Required behavior

### Typed provisioning

The harness must validate a complete immutable specification before reserving capacity. The reuse
key must include every behavior-affecting option and must be independent of map insertion order.
Typed options must own their configuration keys; a free-form override must not replace a key owned
by version-independent topology, transport, security, addressing, or startup behavior.

Each physical cluster must receive a unique name, non-conflicting loopback address range, and
configuration directory. Provisioning must create the requested datacenters and racks, configure
the requested transports and security backends, generate per-run TLS material when HTTPS is
enabled, start all nodes, and wait for each requested Alternator endpoint to become ready.

### Connections and resource scopes

A lease must expose a seed endpoint, all node endpoints, credentials when authorization is
enforced, and the certificate authority when HTTPS is selected. Requesting a transport not enabled
by the specification must fail.

Every lease must receive a unique resource prefix. Generated table names must use only ASCII
letters, digits, underscore, hyphen, and period, and must remain within the service length limit.
Releasing a reusable lease must remove every table in its resource scope before that cluster can be
reused.

### Reusable leases

Concurrent requests with identical reuse keys must share one provisioning operation and one
physical cluster while receiving independent resource scopes. Reusable leases must expose a
read-only cluster view and must not expose node lifecycle or topology mutation.

An idle reusable cluster must pass endpoint health validation before reuse. Failed resource cleanup
or failed health validation must poison and remove the cluster instead of returning it to the pool.

### Private leases

A private lease must own a distinct physical cluster and may expose cluster start, cluster stop,
node start, node stop, node addition, and node removal. Mutations must be serialized. Node addition
must reserve capacity first and roll back both CCM state and capacity after a failure. Removing a
node must release its capacity only after CCM state is removed successfully.

### Admission and eviction

Capacity must be reserved before provisioning and held until the physical cluster is removed.
Requests that can never fit must fail immediately. Other requests may wait for active leases to
release capacity. The scheduler may evict the least-recently-used idle reusable cluster to satisfy
a different request.

Private node addition must evict idle reusable clusters when possible but must fail immediately if
the needed capacity is held by active leases, avoiding a wait while the caller retains another
reservation.

### Failure handling and diagnostics

A failed provisioning or node-add operation must attempt transactional rollback. If rollback also
fails, the harness must retain the cluster identity, capacity reservation, and address lock so a
later cleanup attempt cannot collide with the remaining processes.

Normal and emergency cleanup must collect command output and node diagnostics, stop or remove every
owned cluster, and retry removal where safe. Cleanup must reject directory paths outside the
harness-owned run-directory patterns. Test cancellation must terminate and reap child processes
before rollback begins.

## Interactions with other features

### TLS configuration

The generated certificate authority and node certificates must allow the client TLS feature to
exercise custom trust without disabling certificate validation. Added nodes must receive
certificates before they start.

### Authentication and header optimization

Secured clusters must expose working signing credentials and reject incorrect credentials. This
allows authentication and header-filtering tests to exercise their real wire behavior.

### Discovery, routing, and node health

Connection metadata must reflect the requested datacenter and rack layout so discovery, scoped
routing, and health behavior can be tested against actual topology. Private lifecycle operations
may intentionally change discovery and health results.

### Test parallelism

Reusable cluster sharing and resource scopes must permit independent tests to run concurrently.
Address locks must prevent separate test processes on one host from selecting the same loopback
range.

## Edge cases

- An empty transport set, empty topology, zero-sized rack, or non-positive node resource must fail.
- Semantically identical override maps in different insertion orders must have the same reuse key.
- Different security, topology, transport, resource, version, or override settings must not reuse a
  physical cluster.
- Two concurrent callers arriving during initial provisioning must observe the same result.
- Caller interruption must not cancel shared provisioning needed by another lease.
- Releasing one of several reusable leases must not destroy the shared physical cluster.
- A cleanup failure must prevent reuse even when other leases release successfully.
- An unhealthy idle cluster must be replaced rather than leased again.
- A node-add rollback failure must keep the added node represented for later cleanup.
- Cleanup must be idempotent, and a repeated close must not release capacity twice.
- HTTPS-only clusters must use HTTPS for resource cleanup.
- Resource hints containing Unicode or punctuation outside the service alphabet must be sanitized.

## Conformance requirements

### CCM-REQ-001: Typed specification

The harness must validate immutable typed specifications and derive deterministic, complete reuse
identity from them.

### CCM-REQ-002: Native provisioning

The harness must provision the requested native topology, transports, TLS, security, endpoints, and
credentials through CCM without a container runtime.

### CCM-REQ-003: Reusable isolation

Matching reusable leases must share healthy physical clusters while retaining independent resource
scopes, read-only controls, and cleanup boundaries.

### CCM-REQ-004: Private lifecycle

Private leases must exclusively expose serialized cluster and node lifecycle changes with
transactional capacity accounting.

### CCM-REQ-005: Resource-aware admission

The scheduler must enforce detected memory and the nine-node ceiling, reserve capacity before
provisioning, evict idle clusters by least-recent use, and avoid private-expansion deadlock.

### CCM-REQ-006: Recoverable cleanup

Provisioning and cleanup must preserve ownership after partial failure, collect diagnostics, reap
cancelled commands, reject unsafe cleanup paths, and retry owned cluster removal safely.
